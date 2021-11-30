import os
import shutil
import traceback
import re
import requests

from time import sleep


from cloudal.utils import get_logger, execute_cmd, parse_config_file, getput_file, ExecuteCommandException
from cloudal.action import performing_actions
from cloudal.provisioner import ovh_provisioner
from cloudal.configurator import kubernetes_configurator, k8s_resources_configurator, packages_configurator
from cloudal.experimenter import create_paramsweeper, define_parameters, get_results

from execo_engine import slugify
from kubernetes import config
import yaml

logger = get_logger()


class CancelCombException(Exception):
    pass


class elmerfs_eval_ovh(performing_actions):
    def __init__(self):
        super(elmerfs_eval_ovh, self).__init__()
        self.args_parser.add_argument("--node_ids_file", dest="node_ids_file",
                                      help="the path to the file contents list of node IDs",
                                      default=None,
                                      type=str)
        self.args_parser.add_argument("--kube_master", dest="kube_master",
                                      help="name of kube master node",
                                      default=None,
                                      type=str)
        self.args_parser.add_argument("--setup-k8s-env", dest="setup_k8s_env",
                                      help="create namespace, setup label and volume for kube_workers for the experiment environment",
                                      action="store_true")
        self.args_parser.add_argument("--monitoring", dest="monitoring",
                                      help="deploy Grafana and Prometheus for monitoring",
                                      action="store_true")
        self.args_parser.add_argument("--attach_volume", dest="attach_volume",
                                      help="attach an external volume to every data node",
                                      action="store_true")

    def save_results(self, comb, hosts):
        logger.info("----------------------------------")
        logger.info("5. Starting dowloading the results")

        comb_dir = get_results(comb=comb,
                               hosts=hosts,
                               remote_result_files=['/tmp/results/*'],
                               local_result_dir=self.configs['exp_env']['results_dir'])

    def run_filebench(self, hosts):
        logger.info("Dowloading Filebench configuration file")
        cmd = "wget https://raw.githubusercontent.com/filebench/filebench/master/workloads/varmail.f -P /tmp/ -N"
        execute_cmd(cmd, hosts)
        logger.info('Editing the configuration file')
        cmd = 'sed -i "s/tmp/tmp\/dc-$(hostname)/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        cmd = 'rm -rf /tmp/dc-$(hostname)/bigfileset'
        execute_cmd(cmd, hosts)
        logger.info('Starting filebench')
        cmd = "setarch $(arch) -R filebench -f /tmp/varmail.f > /tmp/results/filebench_$(hostname)"
        execute_cmd(cmd, hosts)
        return True

    def run_benchmark(self, comb, elmerfs_hosts):
        benchmark = comb['benchmarks']
        logger.info('--------------------------------------')
        logger.info("4. Starting benchmark: %s" % benchmark)
        if benchmark == "filebench":
            return self.run_filebench(elmerfs_hosts)

    def deploy_elmerfs(self, kube_master, kube_namespace, elmerfs_hosts):
        logger.info('--------------------------------------')
        logger.info("3. Starting deploying elmerfs on hosts")

        configurator = packages_configurator()
        configurator.install_packages(["libfuse2", "jq"], elmerfs_hosts)

        elmerfs_repo = self.configs["exp_env"]["elmerfs_repo"]
        elmerfs_version = self.configs["exp_env"]["elmerfs_version"]
        elmerfs_file_path = self.configs["exp_env"]["elmerfs_path"]

        if elmerfs_repo is None:
            elmerfs_repo = "https://github.com/scality/elmerfs"
        if elmerfs_version is None:
            elmerfs_version = "latest"

        logger.info("Killing elmerfs process if it is running")
        for host in elmerfs_hosts:
            cmd = "pidof elmerfs"
            _, r = execute_cmd(cmd, host)
            pids = r.processes[0].stdout.strip().split(" ")
            if len(pids) >= 1 and pids[0] != '':
                for pid in pids:
                    cmd = "kill %s" % pid.strip()
                    execute_cmd(cmd, host)
                sleep(5)

            cmd = "mount | grep /tmp/dc-$(hostname)"
            _, r = execute_cmd(cmd, host)
            is_mount = r.processes[0].stdout.strip()

            if is_mount:
                execute_cmd(cmd, host)
                cmd = '''umount /tmp/dc-$(hostname) &&
                         rm -rf /tmp/dc-$(hostname) '''
                execute_cmd(cmd, host)

        logger.info("Delete elmerfs project folder on host (if existing)")
        cmd = "rm -rf /tmp/elmerfs_repo"
        execute_cmd(cmd, kube_master)

        if elmerfs_file_path is None:
            logger.info("Downloading elmerfs project from the repo")
            cmd = """curl \
                    -H "Accept: application/vnd.github.v3+json" \
                    https://api.github.com/repos/scality/elmerfs/releases/%s | jq ".tag_name" \
                    | xargs -I tag_name git clone https://github.com/scality/elmerfs.git --branch tag_name --single-branch /tmp/elmerfs_repo """ % elmerfs_version
            execute_cmd(cmd, kube_master)

            cmd = "cd /tmp/elmerfs_repo \
                && git submodule update --init --recursive"
            execute_cmd(cmd, kube_master)

            cmd = """cat <<EOF | sudo tee /tmp/elmerfs_repo/Dockerfile
                    FROM rust:1.47
                    RUN mkdir  /elmerfs
                    WORKDIR /elmerfs
                    COPY . .
                    RUN apt-get update \
                        && apt-get -y install libfuse-dev
                    RUN cargo build --release
                    CMD ["/bin/bash"]
                    """
            execute_cmd(cmd, kube_master)

            logger.info("Building elmerfs")
            cmd = " cd /tmp/elmerfs_repo/ \
                    && docker build -t elmerfs ."
            execute_cmd(cmd, kube_master)

            cmd = "docker run --name elmerfs elmerfs \
                    && docker cp -L elmerfs:/elmerfs/target/release/main /tmp/elmerfs \
                    && docker rm elmerfs"
            execute_cmd(cmd, kube_master)

            getput_file(hosts=[kube_master],
                        file_paths=["/tmp/elmerfs"],
                        dest_location="/tmp",
                        action="get",)
            elmerfs_file_path = "/tmp/elmerfs"

        logger.info("Uploading elmerfs binary file from %s on local machine to %s elmerfs hosts" %
                    (elmerfs_file_path, len(elmerfs_hosts)))
        getput_file(hosts=elmerfs_hosts,
                    file_paths=[elmerfs_file_path],
                    dest_location="/tmp",
                    action="put",)
        cmd = "chmod +x /tmp/elmerfs \
               && mkdir -p /tmp/dc-$(hostname)"
        execute_cmd(cmd, elmerfs_hosts)

        cmd = 'wget https://raw.githubusercontent.com/scality/elmerfs/master/Elmerfs.template.toml -P /tmp/ -N'
        execute_cmd(cmd, elmerfs_hosts)

        logger.debug("Getting IP of antidoteDB instances on nodes")
        antidote_ips = dict()
        configurator = k8s_resources_configurator()
        pod_list = configurator.get_k8s_resources(resource="pod",
                                                  label_selectors="app=antidote",
                                                  kube_namespace=kube_namespace)
        for pod in pod_list.items:
            host_ip = pod.status.host_ip
            if host_ip not in antidote_ips:
                antidote_ips[host_ip] = list()
            antidote_ips[host_ip].append(pod.status.pod_ip)

        elmerfs_cluster_id = set(range(0, len(self.configs['exp_env']['clusters'])))
        elmerfs_node_id = set(range(0, len(elmerfs_hosts)))
        elmerfs_uid = set(range(len(elmerfs_hosts), len(elmerfs_hosts)*2))

        logger.info('Editing the elmerfs configuration file on %s hosts' % len(elmerfs_hosts))
        for cluster, nodes in self.clusters.items():
            cluster_id = elmerfs_cluster_id.pop()
            for node in nodes:
                host = node['ipAddresses'][0]['ip']
                logger.debug('Editing the configuration file on host %s' % host)
                logger.debug('elmerfs_node_id = %s, elmerfs_cluster_id = %s' %
                             (elmerfs_node_id, elmerfs_cluster_id))
                ips = " ".join([ip for ip in antidote_ips[host]])
                cmd = '''sed -i 's/127.0.0.1:8101/%s:8087/g' /tmp/Elmerfs.template.toml ;
                        sed -i 's/node_id = 0/node_id = %s/g' /tmp/Elmerfs.template.toml ;
                        sed -i 's/cluster_id = 0/cluster_id = %s/g' /tmp/Elmerfs.template.toml
                    ''' % (ips, elmerfs_node_id.pop(), cluster_id)
                execute_cmd(cmd, host)

        logger.info('Running bootstrap command on host %s' % elmerfs_hosts[0])
        cmd = '/tmp/elmerfs --config /tmp/Elmerfs.template.toml --bootstrap --mount /tmp/dc-$(hostname)'
        execute_cmd(cmd, elmerfs_hosts[0])
        # waiting for the bootstrap common to propagate on all DCs
        sleep(30)

        logger.info("Starting elmerfs on %s hosts" % len(elmerfs_hosts))
        for host in elmerfs_hosts:
            elmerfs_cmd = "RUST_BACKTRACE=1 RUST_LOG=debug nohup /tmp/elmerfs --config /tmp/Elmerfs.template.toml --mount=/tmp/dc-$(hostname) --force-view=%s > /tmp/elmer.log 2>&1" % elmerfs_uid.pop(
            )
            logger.debug("Starting elmerfs on %s with cmd: %s" % (host, elmerfs_cmd))
            execute_cmd(elmerfs_cmd, host, mode='start')
            sleep(5)

            logger.info('Checking if elmerfs is running on host %s' % host)
            for i in range(10):
                cmd = "pidof elmerfs"
                _, r = execute_cmd(cmd, host)
                pid = r.processes[0].stdout.strip().split(" ")

                if len(pid) >= 1 and pid[0].strip():
                    logger.info('elmerfs starts successfully')
                    break
                else:
                    logger.info('---> Retrying: starting elmerfs again')
                    execute_cmd(elmerfs_cmd, host, mode="start")
                    sleep(5)
            else:
                logger.info("Cannot deploy elmerfs on host %s" % host)
                return False

        logger.info("Finish deploying elmerfs\n")
        return True

    def _calculate_ring_size(self, n_nodes):
        # calculate the ring size base on the number of nodes in a DC
        # this setting follows the recomandation of Riak KV here:
        # https://docs.riak.com/riak/kv/latest/setup/planning/cluster-capacity/index.html#ring-size-number-of-partitions
        if n_nodes < 7:
            return 64
        elif n_nodes < 10:
            return 128
        elif n_nodes < 14:
            return 256
        elif n_nodes < 20:
            return 512
        elif n_nodes < 40:
            return 1024
        return 2048

    def deploy_antidote(self, kube_namespace, comb):
        logger.info('--------------------------------------')
        logger.info('2. Starting deploying Antidote cluster')
        antidote_k8s_dir = self.configs['exp_env']['antidote_yaml_path']

        logger.debug('Delete old createDC, connectDCs_antidote and exposer-service files if exists')
        for filename in os.listdir(antidote_k8s_dir):
            if filename.startswith('createDC_') or filename.startswith('statefulSet_') or filename.startswith('exposer-service_') or filename.startswith('connectDCs_antidote'):
                if '.template' not in filename:
                    try:
                        os.remove(os.path.join(antidote_k8s_dir, filename))
                    except OSError:
                        logger.debug("Error while deleting file")

        statefulSet_files = [os.path.join(antidote_k8s_dir, 'headlessService.yaml')]
        logger.debug('Modify the statefulSet file')

        file_path = os.path.join(antidote_k8s_dir, 'statefulSet.yaml.template')

        ring_size = self._calculate_ring_size(comb['n_antidotedb_per_dc'])
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster in self.configs['exp_env']['clusters']:
            doc['spec']['replicas'] = comb["n_antidotedb_per_dc"]
            doc['metadata']['name'] = 'antidote-%s' % cluster.lower()
            doc['spec']['template']['spec']['nodeSelector'] = {
                'service_ovh': 'antidote', 'cluster_ovh': '%s' % cluster}
            envs = doc['spec']['template']['spec']['containers'][0]['env']
            for env in envs:
                if env.get('name') == "RING_SIZE":
                    env['value'] = str(ring_size)
                    break
            file_path = os.path.join(antidote_k8s_dir, 'statefulSet_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            statefulSet_files.append(file_path)

        logger.info("Starting AntidoteDB instances")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        configurator.deploy_k8s_resources(files=statefulSet_files, namespace=kube_namespace)

        logger.info('Waiting until all Antidote instances are up')
        deploy_ok = configurator.wait_k8s_resources(resource='pod',
                                                    label_selectors="app=antidote",
                                                    timeout=600,
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            raise CancelCombException("Cannot deploy enough Antidotedb instances")

        logger.debug('Creating createDc.yaml file for each Antidote DC')
        dcs = dict()
        for cluster in self.configs['exp_env']['clusters']:
            dcs[cluster.lower()] = list()
        antidote_list = configurator.get_k8s_resources_name(resource='pod',
                                                            label_selectors='app=antidote',
                                                            kube_namespace=kube_namespace)
        logger.info("Checking if AntidoteDB are deployed correctly")
        if len(antidote_list) != comb['n_antidotedb_per_dc']*len(self.configs['exp_env']['clusters']):
            logger.info("n_antidotedb = %s, n_deployed_antidotedb = %s" %
                        (comb['n_antidotedb_per_dc']*len(self.configs['exp_env']['clusters']), len(antidote_list)))
            raise CancelCombException("Cannot deploy enough Antidotedb instances")

        for antidote in antidote_list:
            cluster = antidote.split('-')[1].strip()
            dcs[cluster].append(antidote)

        file_path = os.path.join(antidote_k8s_dir, 'createDC.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)

        antidote_masters = list()
        createdc_files = list()
        for cluster, pods in dcs.items():
            doc['spec']['template']['spec']['containers'][0]['args'] = ['--createDc',
                                                                        '%s.antidote:8087' % pods[0]] + ['antidote@%s.antidote' % pod for pod in pods]
            doc['metadata']['name'] = 'createdc-%s' % cluster.lower()
            antidote_masters.append('%s.antidote:8087' % pods[0])
            file_path = os.path.join(antidote_k8s_dir, 'createDC_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            createdc_files.append(file_path)

        logger.debug('Creating exposer-service.yaml files')
        file_path = os.path.join(antidote_k8s_dir, 'exposer-service.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster, pods in dcs.items():
            doc['spec']['selector']['statefulset.kubernetes.io/pod-name'] = pods[0]
            doc['metadata']['name'] = 'antidote-exposer-%s' % cluster.lower()
            file_path = os.path.join(antidote_k8s_dir, 'exposer-service_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            createdc_files.append(file_path)

        logger.info("Creating Antidote DCs and exposing services")
        configurator.deploy_k8s_resources(files=createdc_files, namespace=kube_namespace)

        logger.info('Waiting until all antidote DCs are created')
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors='app=antidote',
                                                    kube_namespace=kube_namespace)

        if not deploy_ok:
            raise CancelCombException("Cannot connect Antidotedb instances to create DC")

        logger.debug('Creating connectDCs_antidote.yaml to connect all Antidote DCs')
        file_path = os.path.join(antidote_k8s_dir, 'connectDCs.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        doc['spec']['template']['spec']['containers'][0]['args'] = [
            '--connectDcs'] + antidote_masters
        file_path = os.path.join(antidote_k8s_dir, 'connectDCs_antidote.yaml')
        with open(file_path, 'w') as f:
            yaml.safe_dump(doc, f)

        logger.info("Connecting all Antidote DCs into a cluster")
        configurator.deploy_k8s_resources(files=[file_path], namespace=kube_namespace)

        logger.info('Waiting until connecting all Antidote DCs')
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors='app=antidote',
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            raise CancelCombException("Cannot connect all Antidotedb DCs")

        logger.info('Finish deploying the Antidote cluster')

    def deploy_antidote_monitoring(self, kube_master, kube_namespace):
        logger.info('--------------------------------------')
        logger.info("Deploying monitoring system")
        monitoring_k8s_dir = self.configs['exp_env']['monitoring_yaml_path']

        logger.info("Deleting old deployment")
        cmd = "rm -rf /root/antidote_stats"
        execute_cmd(cmd, kube_master)

        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()

        cmd = "git clone https://github.com/AntidoteDB/antidote_stats.git"
        execute_cmd(cmd, kube_master)
        logger.info("Setting to allow pods created on kube_master")
        cmd = "kubectl taint nodes --all node-role.kubernetes.io/master-"
        execute_cmd(cmd, kube_master, is_continue=True)

        pods = configurator.get_k8s_resources_name(resource='pod',
                                                   label_selectors='app=antidote',
                                                   kube_namespace=kube_namespace)
        antidote_info = ["%s.antidote:3001" % pod for pod in pods]

        logger.debug('Modify the prometheus.yml file with antidote instances info')
        file_path = os.path.join(monitoring_k8s_dir, 'prometheus.yml.template')
        with open(file_path) as f:
            doc = f.read().replace('antidotedc_info', '%s' % antidote_info)
        prometheus_configmap_file = os.path.join(monitoring_k8s_dir, 'prometheus.yml')
        with open(prometheus_configmap_file, 'w') as f:
            f.write(doc)
        configurator.create_configmap(file=prometheus_configmap_file,
                                      namespace=kube_namespace,
                                      configmap_name='prometheus-configmap')
        logger.debug('Modify the deploy_prometheus.yaml file with kube_master info')
        kube_master_info = configurator.get_k8s_resources(resource='node',
                                                          label_selectors='kubernetes.io/hostname=%s' % kube_master)
        for item in kube_master_info.items[0].status.addresses:
            if item.type == 'InternalIP':
                kube_master_ip = item.address
        file_path = os.path.join(monitoring_k8s_dir, 'deploy_prometheus.yaml.template')
        with open(file_path) as f:
            doc = f.read().replace('kube_master_ip', '%s' % kube_master_ip)
            doc = doc.replace("kube_master_hostname", '%s' % kube_master)
        prometheus_deploy_file = os.path.join(monitoring_k8s_dir, 'deploy_prometheus.yaml')
        with open(prometheus_deploy_file, 'w') as f:
            f.write(doc)

        logger.info("Starting Prometheus service")
        configurator.deploy_k8s_resources(files=[prometheus_deploy_file], namespace=kube_namespace)
        logger.info('Waiting until Prometheus instance is up')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app=prometheus",
                                        kube_namespace=kube_namespace)

        logger.debug('Modify the deploy_grafana.yaml file with kube_master info')
        file_path = os.path.join(monitoring_k8s_dir, 'deploy_grafana.yaml.template')
        with open(file_path) as f:
            doc = f.read().replace('kube_master_ip', '%s' % kube_master_ip)
            doc = doc.replace("kube_master_hostname", '%s' % kube_master)
        grafana_deploy_file = os.path.join(monitoring_k8s_dir, 'deploy_grafana.yaml')
        with open(grafana_deploy_file, 'w') as f:
            f.write(doc)

        file = '/root/antidote_stats/monitoring/grafana-config/provisioning/datasources/all.yml'
        cmd = """ sed -i "s/localhost/%s/" %s """ % (kube_master_ip, file)
        execute_cmd(cmd, kube_master)

        logger.info("Starting Grafana service")
        configurator.deploy_k8s_resources(files=[grafana_deploy_file], namespace=kube_namespace)
        logger.info('Waiting until Grafana instance is up')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app=grafana",
                                        kube_namespace=kube_namespace)

        logger.info("Finish deploying monitoring system\n")
        prometheus_url = "http://%s:9090" % kube_master_ip
        grafana_url = "http://%s:3000" % kube_master_ip
        logger.info("Connect to Grafana at: %s" % grafana_url)
        logger.info("Connect to Prometheus at: %s" % prometheus_url)

        return prometheus_url, grafana_url

    def get_prometheus_metric(self, metric_name, prometheus_url):
        logger.info('---------------------------')
        logger.info('Retrieving Prometheus data')
        query = "%s/api/v1/query?query=%s" % (prometheus_url, metric_name)
        logger.debug("query = %s" % query)
        r = requests.get(query)
        normalize_result = dict()
        logger.debug("status_code = %s" % r.status_code)
        if r.status_code == 200:
            result = r.json()
            for each in result['data']['result']:
                key = each['metric']['instance']
                normalize_result[key] = int(each['value'][1])
        return normalize_result

    def clean_exp_env(self, kube_namespace, elmerfs_hosts):
        logger.info('1. Deleting all k8s resource from the previous run in namespace "%s"' %
                    kube_namespace)
        logger.info(
            'Delete namespace "%s" to delete all the resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

        if len(elmerfs_hosts) > 0:
            logger.debug('Delete all files in /tmp/results folder on elmerfs nodes')
            cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
            execute_cmd(cmd, elmerfs_hosts)

    def run_exp_workflow(self, kube_namespace, comb, kube_master, sweeper):
        configurator = k8s_resources_configurator()
        antidote_nodes_info = configurator.get_k8s_resources(resource='node',
                                                             label_selectors='service_ovh=antidote',
                                                             kube_namespace=kube_namespace)
        antidote_hosts = [r.metadata.annotations['flannel.alpha.coreos.com/public-ip']
                          for r in antidote_nodes_info.items]
        elmerfs_hosts = antidote_hosts

        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(kube_namespace, elmerfs_hosts)
            self.deploy_antidote(kube_namespace, comb)
            is_elmerfs = self.deploy_elmerfs(kube_master, kube_namespace, elmerfs_hosts)
            if is_elmerfs:
                if self.args.monitoring:
                    prometheus_url, _ = self.deploy_antidote_monitoring(kube_master, kube_namespace)
                is_finished = self.run_benchmark(comb, elmerfs_hosts)
                if is_finished:
                    comb_ok = True
                    self.save_results(comb, elmerfs_hosts)
            else:
                raise CancelCombException("Cannot deploy elmerfs")
        except (ExecuteCommandException, CancelCombException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = False
        finally:
            if comb_ok:
                sweeper.done(comb)
                logger.info('Finish combination: %s' % slugify(comb))
            else:
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' is canceled')
            logger.info('%s combinations remaining\n' % len(sweeper.get_remaining()))
        return sweeper

    def install_filebench(self, hosts):
        configurator = packages_configurator()
        configurator.install_packages(["build-essential", "bison", "flex", "libtool"], hosts)

        cmd = "wget https://github.com/filebench/filebench/archive/refs/tags/1.5-alpha3.tar.gz -P /tmp/ -N"
        execute_cmd(cmd, hosts)
        cmd = "tar -xf /tmp/1.5-alpha3.tar.gz --directory /tmp/"
        execute_cmd(cmd, hosts)
        cmd = '''cd /tmp/filebench-1.5-alpha3/ &&
                 libtoolize &&
                 aclocal &&
                 autoheader &&
                 automake --add-missing &&
                 autoconf &&
                 ./configure &&
                 make &&
                 make install'''
        execute_cmd(cmd, hosts)

    def _setup_ovh_kube_volumes(self, kube_workers, n_pv=3):
        logger.info("Setting volumes on %s kubernetes workers" % len(kube_workers))
        cmd = '''rm -rf /rmp/pv'''
        execute_cmd(cmd, kube_workers)
        logger.debug('Create n_pv partitions on the physical disk to make a PV can be shared')
        cmd = '''for i in $(seq 1 %s); do
                     mkdir -p /tmp/pv/vol${i}
                     mkdir -p /mnt/disks/vol${i}
                     mount --bind /tmp/pv/vol${i} /mnt/disks/vol${i}
                 done''' % n_pv
        execute_cmd(cmd, kube_workers)

        # TOTO: check if existing, delete first
        logger.info("Creating local persistance volumes on Kubernetes cluster")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs['exp_env']['antidote_yaml_path']
        deploy_files = [os.path.join(antidote_k8s_dir, 'local_persistentvolume.yaml'),
                        os.path.join(antidote_k8s_dir, 'storageClass.yaml')]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info('Waiting for setting local persistance volumes')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app.kubernetes.io/instance=local-volume-provisioner")

    def _set_kube_workers_label(self, kube_master):
        configurator = k8s_resources_configurator()
        for node in self.nodes:
            if node['ipAddresses'][0]['ip'] == kube_master:
                pass
            else:
                r = configurator.set_labels_node(nodename=node['name'],
                                                 labels='cluster_ovh=%s,service_ovh=antidote' % node['region'])
                if r is None:
                    logger.info('Cannot Set labels for kubernetes workers')
                    exit()

    def _get_credential(self, kube_master):
        home = os.path.expanduser('~')
        kube_dir = os.path.join(home, '.kube')
        if not os.path.exists(kube_dir):
            os.mkdir(kube_dir)
        getput_file(hosts=[kube_master],
                    file_paths=['~/.kube/config'],
                    dest_location=kube_dir,
                    action='get')
        kube_config_file = os.path.join(kube_dir, 'config')
        config.load_kube_config(config_file=kube_config_file)
        logger.info('Kubernetes config file is stored at: %s' % kube_config_file)

    def deploy_k8s(self, kube_master):
        logger.debug("Init configurator: kubernetes_configurator")
        configurator = kubernetes_configurator(hosts=self.hosts, kube_master=kube_master)
        _, kube_workers = configurator.deploy_kubernetes_cluster()

        return kube_workers

    def setup_k8s_env(self, kube_master, kube_namespace, kube_workers):
        self._get_credential(kube_master)

        logger.info('Create k8s namespace "%s" for this experiment' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.create_namespace(namespace=kube_namespace)

        logger.info('Set labels for all kubernetes workers')
        self._set_kube_workers_label(kube_master)

        self._setup_ovh_kube_volumes(kube_workers, n_pv=3)

        logger.info("Finish deploying the Kubernetes cluster")

    def config_host(self, kube_master, kube_namespace):
        logger.info("Starting configuring nodes")

        # configuring Kubernetes environment
        kube_workers = [host for host in self.hosts if host != kube_master]
        if self.args.kube_master is None:
            kube_workers = self.deploy_k8s(kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        elif self.args.setup_k8s_env:
            logger.info('Kubernetes master: %s' % kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        else:
            self._get_credential(kube_master)
            if self.args.attach_volume:
                self._setup_ovh_kube_volumes(kube_workers, n_pv=3)

        # Installing benchmark for running the experiments
        if self.configs['parameters']['benchmarks'] == 'filebench':
            logger.info('Installing Filebench')
            self.install_filebench(kube_workers)

        logger.info("Finish configuring nodes")
        return kube_master

    def setup_env(self, kube_master_site, kube_namespace):
        logger.info("STARTING SETTING THE EXPERIMENT ENVIRONMENT")
        logger.info("Starting provisioning nodes on OVHCloud")

        provisioner = ovh_provisioner(configs=self.configs, node_ids_file=self.args.node_ids_file)
        provisioner.provisioning()

        self.nodes = provisioner.nodes
        self.hosts = provisioner.hosts
        node_ids_file = provisioner.node_ids_file

        kube_master = self.args.kube_master
        if kube_master is None:
            for node in self.nodes:
                if node['region'] == kube_master_site:
                    kube_master = node['ipAddresses'][0]['ip']
                    kube_master_id = node['id']
                    break
        else:
            for node in self.nodes:
                if node['ipAddresses'][0]['ip'] == kube_master:
                    kube_master_id = node['id']
                    break
        logger.info('Kubernetes master: %s' % kube_master)

        data_nodes = list()
        self.clusters = dict()
        for node in self.nodes:
            if node['id'] == kube_master_id:
                continue
            cluster = node['region']
            self.clusters[cluster] = [node] + self.clusters.get(cluster, list())
        for region, nodes in self.clusters.items():
            data_nodes += nodes[0: max(self.normalized_parameters['n_antidotedb_per_dc'])]
        data_hosts = [node['ipAddresses'][0]['ip'] for node in data_nodes]

        if self.args.attach_volume:
            logger.info('Attaching external volumes to %s nodes' % len(data_nodes))
            provisioner.attach_volume(nodes=data_nodes)

            logger.info('Formatting the new external volumes')
            cmd = '''disk=$(ls -lt /dev/ | grep '^b' | head -n 1 | awk {'print $NF'})
                   mkfs.ext4 -F /dev/$disk;
                   mount -t ext4 /dev/$disk /tmp;
                   chmod 777 /tmp'''
            execute_cmd(cmd, data_hosts)

        self.config_host(kube_master, kube_namespace)

        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return kube_master, node_ids_file

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = max(self.normalized_parameters['n_antidotedb_per_dc'])

        # create standard cluster information to make reservation on OVHCloud, this info using by OVH provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            if cluster == kube_master_site:
                clusters.append({'region': cluster,
                                'n_nodes': n_nodes_per_cluster + 1,
                                 'instance_type': self.configs['instance_type'],
                                 'flexible_instance': self.configs['flexible_instance'],
                                 'image': self.configs['image']})
            else:
                clusters.append({'region': cluster,
                                'n_nodes': n_nodes_per_cluster,
                                 'instance_type': self.configs['instance_type'],
                                 'flexible_instance': self.configs['flexible_instance'],
                                 'image': self.configs['image']})
        self.configs['clusters'] = clusters

        # copy all YAML template folders to a new one for this experiment run to avoid conflicting
        results_dir_name = (self.configs["exp_env"]["results_dir"]).split('/')[-1]
        antidote_yaml_path = self.configs["exp_env"]["antidote_yaml_path"]
        old_path = os.path.dirname(antidote_yaml_path)
        new_path = old_path + "_" + results_dir_name
        if os.path.exists(new_path):
            shutil.rmtree(new_path)
        shutil.copytree(old_path, new_path)

        self.configs["exp_env"]["antidote_yaml_path"] = new_path + "/antidotedb_yaml"
        self.configs["exp_env"]["monitoring_yaml_path"] = new_path + "/monitoring_yaml"

        return kube_master_site

    def run(self):
        logger.debug('Parse and convert configs for OVH provisioner')
        self.configs = parse_config_file(self.args.config_file_path)
        # Add the number of Antidote DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        kube_master_site = self.create_configs()

        logger.info('''Your largest topology:
                        Antidote DCs: %s
                        n_antidotedb_per_DC: %s''' % (
            len(self.configs['exp_env']['clusters']),
            max(self.normalized_parameters['n_antidotedb_per_dc'])
        )
        )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)

        kube_namespace = 'elmerfs-exp'
        node_ids_file = None
        while len(sweeper.get_remaining()) > 0:
            if node_ids_file is None:
                kube_master, node_ids_file = self.setup_env(kube_master_site, kube_namespace)
            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(kube_namespace=kube_namespace,
                                            kube_master=kube_master,
                                            comb=comb,
                                            sweeper=sweeper)
            # if not is_nodes_alive(node_ids_file):
            #     node_ids_file = None
        logger.info('Finish the experiment!!!')


if __name__ == "__main__":
    logger.info("Init engine in %s" % __file__)
    engine = elmerfs_eval_ovh()

    try:
        logger.info("Start engine in %s" % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')
