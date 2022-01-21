from logging import raiseExceptions
import os
import shutil
import traceback
import random

from time import sleep

from cloudal.utils import (
    get_logger,
    execute_cmd,
    parse_config_file,
    getput_file,
    ExecuteCommandException,
)
from cloudal.action import performing_actions_g5k
from cloudal.provisioner import g5k_provisioner
from cloudal.configurator import (
    kubernetes_configurator,
    k8s_resources_configurator,
    packages_configurator,
)
from cloudal.experimenter import create_combs_queue, is_job_alive, get_results, define_parameters

from execo_g5k import oardel
from execo_engine import slugify
from kubernetes import config
import yaml

logger = get_logger()


class CancelCombException(Exception):
    pass


class elmerfs_eval_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(elmerfs_eval_g5k, self).__init__()
        self.args_parser.add_argument("--kube_master",
                                      dest="kube_master",
                                      help="name of kube master node",
                                      default=None,
                                      type=str,)
        self.args_parser.add_argument("--monitoring", dest="monitoring",
                                      help="deploy Grafana and Prometheus for monitoring",
                                      action="store_true")

    def deploy_monitoring(self, kube_master, kube_namespace):
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
        logger.info("Connect to Grafana at: http://%s:3000" % kube_master_ip)
        logger.info("Connect to Prometheus at: http://%s:9090" % kube_master_ip)

    def clean_exp_env(self, kube_namespace):
        logger.info('1. Deleting all k8s resource from the previous run in namespace "%s"' % kube_namespace)
        logger.info('Delete namespace "%s" to delete all the resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

    def run_mailserver(self, elmerfs_hosts, duration, n_client):
        if n_client == 100:
            n_hosts = 1
        else:
            n_hosts = n_client

        hosts = random.sample(elmerfs_hosts, n_hosts)
        logger.info("Dowloading Filebench configuration file")
        cmd = "wget https://raw.githubusercontent.com/filebench/filebench/master/workloads/varmail.f -P /tmp/ -N"
        execute_cmd(cmd, hosts)

        logger.info('Editing the configuration file')
        cmd = 'sed -i "s/tmp/tmp\/dc-$(hostname)/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/run 60/run %s/g" /tmp/varmail.f' % duration
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/name=bigfileset/name=bigfileset-$(hostname)/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/meandirwidth=1000000/meandirwidth=1000/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        if n_client != 100:
            cmd = 'sed -i "s/nthreads=16/nthreads=32/g" /tmp/varmail.f'
            execute_cmd(cmd, hosts)

        logger.info('Clearing cache ')
        cmd = 'rm -rf /tmp/dc-$(hostname)/bigfileset'
        execute_cmd(cmd, hosts)
        cmd = 'sync; echo 3 > /proc/sys/vm/drop_caches'
        execute_cmd(cmd, hosts)

        logger.info('hosts = %s' % hosts)
        logger.info('Starting filebench')
        cmd = "setarch $(arch) -R filebench -f /tmp/varmail.f > /tmp/results/filebench_$(hostname)"
        execute_cmd(cmd, hosts)
        return True, hosts

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

    def run_benchmark(self, comb, elmerfs_hosts):
        benchmarks = comb['benchmarks']
        logger.info("-----------------------------------")
        logger.info("4. Running benchmark %s" % benchmarks)

        if benchmarks == "mailserver":
            is_finished, hosts = self.run_mailserver(elmerfs_hosts, comb['duration'], comb['n_client'])
            return is_finished, hosts

    def save_results(self, comb, hosts):
        logger.info("----------------------------------")
        logger.info("5. Starting downloading the results")

        get_results(comb=comb,
                    hosts=hosts,
                    remote_result_files=['/tmp/results/*'],
                    local_result_dir=self.configs['exp_env']['results_dir'])

    def run_workflow(self, kube_namespace, kube_master, comb, sweeper):

        comb_ok = False

        try:
            logger.info("=======================================")
            logger.info("Performing combination: " + slugify(comb))
            self.clean_exp_env(kube_namespace)
            self.deploy_antidote(kube_namespace, comb)

            logger.debug("Getting hosts and IP of antidoteDB instances on their nodes")
            antidote_ips = dict()
            configurator = k8s_resources_configurator()
            pod_list = configurator.get_k8s_resources(resource="pod",
                                                      label_selectors="app=antidote",
                                                      kube_namespace=kube_namespace,)
            for pod in pod_list.items:
                node = pod.spec.node_name
                if node not in antidote_ips:
                    antidote_ips[node] = list()
                antidote_ips[node].append(pod.status.pod_ip)

            antidote_hosts = list(antidote_ips.keys())
            elmerfs_hosts = antidote_hosts

            is_elmerfs = self.deploy_elmerfs(kube_master, kube_namespace, elmerfs_hosts, antidote_ips)
            if is_elmerfs:

                if self.args.monitoring:
                    self.deploy_monitoring(kube_master, kube_namespace)
                is_finished, hosts = self.run_benchmark(comb, elmerfs_hosts)

                if is_finished:
                    comb_ok = True
                    self.save_results(comb, hosts)
            else:
                raise CancelCombException("Cannot deploy elmerfs")
        except (ExecuteCommandException, CancelCombException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = False
        finally:
            if comb_ok:
                sweeper.done(comb)
                logger.info("Finish combination: %s" % slugify(comb))
            else:
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + " is canceled")
            logger.info("%s combinations remaining\n" % len(sweeper.get_remaining()))
        return sweeper

    def deploy_elmerfs(self, kube_master, kube_namespace, elmerfs_hosts, antidote_ips):
        logger.info("-----------------------------------------")
        logger.info("3. Starting deploying elmerfs on %s hosts" % len(elmerfs_hosts))

        logger.debug('Delete all files in /tmp/results folder on elmerfs nodes from the previous run')
        cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
        execute_cmd(cmd, elmerfs_hosts)
        cmd = 'rm -rf /tmp/elmerfs'
        execute_cmd(cmd, elmerfs_hosts)

        elmerfs_repo = self.configs["exp_env"]["elmerfs_repo"]
        elmerfs_version = self.configs["exp_env"]["elmerfs_version"]
        elmerfs_file_path = self.configs["exp_env"]["elmerfs_path"]

        if elmerfs_repo is None:
            elmerfs_repo = "https://github.com/scality/elmerfs"
        if elmerfs_version is None:
            elmerfs_version = "latest"

        logger.info("Killing elmerfs process if it is running")
        logger.info('elmerfs_hosts: %s' % elmerfs_hosts)
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

        logger.info("Uploading elmerfs binary file from local to %s elmerfs hosts"
                    % len(elmerfs_hosts))
        getput_file(hosts=elmerfs_hosts,
                    file_paths=[elmerfs_file_path],
                    dest_location="/tmp",
                    action="put",)
        cmd = "chmod +x /tmp/elmerfs \
               && mkdir -p /tmp/dc-$(hostname)"
        execute_cmd(cmd, elmerfs_hosts)

        cmd = 'wget https://raw.githubusercontent.com/scality/elmerfs/master/Elmerfs.template.toml -P /tmp/ -N'
        execute_cmd(cmd, elmerfs_hosts)

        elmerfs_cluster_id = set(range(0, len(self.configs['exp_env']['clusters'])))
        elmerfs_node_id = set(range(0, len(elmerfs_hosts)))
        elmerfs_uid = set(range(len(elmerfs_hosts), len(elmerfs_hosts)*2))

        logger.info('Editing the elmerfs configuration file on %s hosts' % len(elmerfs_hosts))
        for cluster in self.configs['exp_env']['clusters']:
            configurator = k8s_resources_configurator()
            host_list = configurator.get_k8s_resources(resource="node",
                                                       label_selectors="cluster_g5k=%s" % cluster,
                                                       kube_namespace=kube_namespace)
            hosts = [host.metadata.name for host in host_list.items]
            cluster_id = elmerfs_cluster_id.pop()
            for host in hosts:
                if host in antidote_ips:
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

        logger.info("Finish deploying elmerfs")
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

        ring_size = self._calculate_ring_size(comb['n_nodes_per_dc'])
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster in self.configs['exp_env']['clusters']:
            doc['spec']['replicas'] = comb['n_nodes_per_dc']
            doc['metadata']['name'] = 'antidote-%s' % cluster.lower()
            doc['spec']['template']['spec']['nodeSelector'] = {
                'service_g5k': 'antidote', 'cluster_g5k': '%s' % cluster}
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
        if len(antidote_list) != comb['n_nodes_per_dc']*len(self.configs['exp_env']['clusters']):
            logger.info("n_antidotedb = %s, n_deployed_antidotedb = %s" %
                        (comb['n_nodes_per_dc']*len(self.configs['exp_env']['clusters']), len(antidote_list)))
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
        return 1

    def _set_kube_workers_label(self, kube_workers):
        logger.info("Set labels for all kubernetes workers")
        configurator = k8s_resources_configurator()
        for host in kube_workers:
            cluster = host.split("-")[0]
            labels = "cluster_g5k=%s,service_g5k=antidote" % cluster
            configurator.set_labels_node(host, labels)

    def _setup_g5k_kube_volumes(self, kube_workers, n_pv=3):

        logger.info("Setting volumes on %s kubernetes workers" % len(kube_workers))
        cmd = """umount /dev/sda5;
                 mount -t ext4 /dev/sda5 /tmp"""
        execute_cmd(cmd, kube_workers)
        logger.debug("Create n_pv partitions on the physical disk to make a PV can be shared")
        cmd = """for i in $(seq 1 %s); do
                     mkdir -p /tmp/pv/vol${i}
                     mkdir -p /mnt/disks/vol${i}
                     mount --bind /tmp/pv/vol${i} /mnt/disks/vol${i}
                 done""" % n_pv
        execute_cmd(cmd, kube_workers)

        logger.info("Creating local persistance volumes on Kubernetes cluster")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs["exp_env"]["antidote_yaml_path"]
        deploy_files = [
            os.path.join(antidote_k8s_dir, "local_persistentvolume.yaml"),
            os.path.join(antidote_k8s_dir, "storageClass.yaml"),
        ]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info("Waiting for setting local persistance volumes")
        configurator.wait_k8s_resources(resource="pod",
                                        label_selectors="app.kubernetes.io/instance=local-volume-provisioner",)

    def _get_credential(self, kube_master):
        home = os.path.expanduser("~")
        kube_dir = os.path.join(home, ".kube")
        if not os.path.exists(kube_dir):
            os.mkdir(kube_dir)
        getput_file(hosts=[kube_master],
                    file_paths=["~/.kube/config"],
                    dest_location=kube_dir,
                    action="get",)
        kube_config_file = os.path.join(kube_dir, "config")
        config.load_kube_config(config_file=kube_config_file)
        logger.info("Kubernetes config file is stored at: %s" % kube_config_file)

    def config_kube(self, kube_master, antidote_hosts, kube_namespace):
        logger.info("Starting configuring a Kubernetes cluster")
        logger.debug("Init configurator: kubernetes_configurator")
        configurator = kubernetes_configurator(hosts=self.hosts, kube_master=kube_master)
        configurator.deploy_kubernetes_cluster()

        self._get_credential(kube_master)

        logger.info('Create k8s namespace "%s" for this experiment' % kube_namespace)
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        configurator.create_namespace(kube_namespace)

        kube_workers = [host for host in antidote_hosts if host != kube_master]

        self._setup_g5k_kube_volumes(kube_workers, n_pv=3)

        self._set_kube_workers_label(kube_workers)

        logger.info("Finish configuring the Kubernetes cluster\n")
        return kube_workers

    def config_host(self, kube_master_site, kube_namespace):
        kube_master = self.args.kube_master

        if self.args.kube_master is None:
            antidote_hosts = list()
            for cluster in self.configs['clusters']:
                cluster_name = cluster["cluster"]
                if cluster_name == self.configs["exp_env"]["kube_master_site"]:
                    antidote_hosts += [
                        host for host in self.hosts if host.startswith(cluster_name)
                    ][0: cluster["n_nodes"] + 1]
                else:
                    antidote_hosts += [
                        host for host in self.hosts if host.startswith(cluster_name)
                    ][0: cluster["n_nodes"]]

            for host in antidote_hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break

            kube_workers = self.config_kube(kube_master, antidote_hosts, kube_namespace)
        else:
            logger.info("Kubernetes master: %s" % kube_master)
            self._get_credential(kube_master)

            configurator = k8s_resources_configurator()
            deployed_hosts = configurator.get_k8s_resources(resource="node")
            kube_workers = [host.metadata.name for host in deployed_hosts.items]
            kube_workers.remove(kube_master)

        logger.info('Installing elmerfs dependencies')
        configurator = packages_configurator()
        configurator.install_packages(["libfuse2", "wget", "jq"], kube_workers)
        # Create mount point on elmerfs hosts
        cmd = "mkdir -p /tmp/dc-$(hostname)"
        execute_cmd(cmd, kube_workers)

        # Installing filebench for running the experiments
        if self.configs['parameters']['benchmarks'] in ['mailserver', 'videoserver']:
            logger.info('Installing Filebench')
            self.install_filebench(kube_workers)

    def setup_env(self, kube_master_site, kube_namespace):
        logger.info("Starting configuring the experiment environment")
        logger.debug("Init provisioner: g5k_provisioner")
        provisioner = g5k_provisioner(configs=self.configs,
                                      keep_alive=self.args.keep_alive,
                                      out_of_chart=self.args.out_of_chart,
                                      oar_job_ids=self.args.oar_job_ids,
                                      no_deploy_os=self.args.no_deploy_os,
                                      is_reservation=self.args.is_reservation,
                                      job_name="cloudal",)

        provisioner.provisioning()
        self.hosts = provisioner.hosts
        oar_job_ids = provisioner.oar_result
        self.oar_result = provisioner.oar_result

        logger.info("Starting configuring nodes")
        kube_master = self.args.kube_master
        if kube_master is None:
            for host in self.hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break

        self.config_host(kube_master, kube_namespace)

        logger.info("Finish configuring nodes\n")

        self.args.oar_job_ids = None
        logger.info("Finish configuring the experiment environment\n")
        return oar_job_ids, kube_master

    def create_configs(self):
        logger.debug("Get the k8s master node")
        kube_master_site = self.configs["exp_env"]["kube_master_site"]
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = max(self.normalized_parameters['n_nodes_per_dc'])

        # create standard cluster information to make reservation on Grid'5000, this info using by G5k provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            if cluster == kube_master_site:
                clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster + 1})
            else:
                clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster})
        self.configs['clusters'] = clusters

        # copy all YAML template folders to a new one for this experiment run to avoid conflicting
        results_dir_name = (self.configs["exp_env"]["results_dir"]).split('/')[-1]
        results_dir_path = os.path.dirname(self.configs["exp_env"]["results_dir"])

        yaml_dir_path = os.path.dirname(self.configs["exp_env"]["antidote_yaml_path"])
        yaml_dir_name = yaml_dir_path.split('/')[-1]

        new_yaml_dir_name = yaml_dir_name + "_" + results_dir_name
        new_path = results_dir_path + "/" + new_yaml_dir_name
        if os.path.exists(new_path):
            shutil.rmtree(new_path)
        shutil.copytree(yaml_dir_path, new_path)

        self.configs["exp_env"]["antidote_yaml_path"] = new_path + "/antidotedb_yaml"
        self.configs["exp_env"]["monitoring_yaml_path"] = new_path + "/monitoring_yaml"

        return kube_master_site

    def run(self):
        logger.debug("Parse and convert configs for G5K provisioner")
        self.configs = parse_config_file(self.args.config_file_path)

        # Add the number of Antidote DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        kube_master_site = self.create_configs()

        logger.info('''Your largest topology:
                        Antidote DCs: %s
                        n_antidotedb_per_DC: %s  ''' % (
            len(self.configs['exp_env']['clusters']),
            max(self.normalized_parameters['n_nodes_per_dc'])
        )
        )

        sweeper = create_combs_queue(result_dir=self.configs["exp_env"]["results_dir"],
                                     parameters=self.configs["parameters"],)
        kube_namespace = "elmerfs-exp"
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                oar_job_ids, kube_master = self.setup_env(kube_master_site, kube_namespace)

            comb = sweeper.get_next()
            sweeper = self.run_workflow(kube_master=kube_master,
                                        kube_namespace=kube_namespace,
                                        comb=comb,
                                        sweeper=sweeper,)

            if not is_job_alive(oar_job_ids):
                oardel(oar_job_ids)
                oar_job_ids = None
        logger.info("Finish the experiment!!!")


if __name__ == "__main__":
    logger.info("Init engine in %s" % __file__)
    engine = elmerfs_eval_g5k()

    try:
        logger.info("Start engine in %s" % __file__)
        engine.start()
    except Exception as e:
        logger.error("Program is terminated by the following exception: %s" % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info("Program is terminated by keyboard interrupt.")

    if not engine.args.keep_alive:
        logger.info("Deleting reservation")
        oardel(engine.oar_result)
        logger.info("Reservation deleted")
    else:
        logger.info("Reserved nodes are kept alive for inspection purpose.")
