import os
import traceback
import math

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
from cloudal.experimenter import create_combs_queue, is_job_alive, get_results

from execo_g5k import oardel
from execo_engine import slugify
from kubernetes import config
import yaml

logger = get_logger()


class elmerfs_eval_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(elmerfs_eval_g5k, self).__init__()
        self.args_parser.add_argument("--kube-master",
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

    def clean_exp_env(self, elmerfs_hosts):
        logger.info("--Cleaning experiment environment")
        cmd = "rm -f /tmp/results/*"
        execute_cmd(cmd, elmerfs_hosts)
        cmd = "rm -f /tmp/dc-$(hostname)/*"
        execute_cmd(cmd, elmerfs_hosts)

    def set_latency(self, latency, antidote_dc):
        """Limit the latency in host"""
        logger.info("--Setting network latency=%s on hosts" % latency)
        latency = latency/2
        for cur_cluster, cur_cluster_info in antidote_dc.items():
            other_clusters = {cluster_name: cluster_info
                              for cluster_name, cluster_info in antidote_dc.items() if cluster_name != cur_cluster}

            for _, cluster_info in other_clusters.items():
                for pod_ip in cluster_info['pod_ips']:
                    cmd = "tcset flannel.1 --delay %s --network %s" % (latency, pod_ip)
                    execute_cmd(cmd, cur_cluster_info['host_names'])

    def reset_latency(self, antidote_dc):
        """Delete the Limitation of latency in host"""
        logger.info("--Remove network latency on hosts")
        for _, cluster_info in antidote_dc.items():
            cmd = "tcdel flannel.1 --all"
            execute_cmd(cmd, cluster_info['host_names'])

    def convergence(self, comb, antidote_dc):
        logger.info("----Chose the source and destination hosts")
        index = int(comb['iteration']) % len(antidote_dc.keys())
        cluster_src_name = list(antidote_dc.keys())[index]
        cluster_src = antidote_dc[cluster_src_name]

        index = int(comb['iteration']) % len(cluster_src['host_names'])
        host_src = cluster_src['host_names'][index]

        hosts_dest = list()
        for cluster_name, cluster_info in antidote_dc.items():
            hosts_dest += cluster_info['host_names']
        hosts_dest.remove(host_src)
        logger.info("host_src = %s" % host_src)
        logger.info("hosts_dest = %s" % hosts_dest)

        logger.info("----Start checksum process on destination hosts")
        cmd = "touch /tmp/dc-$(hostname)/sample"
        execute_cmd(cmd, host_src)
        sleep(5)
        cmd = "bash /tmp/convergence_files/periodically_checksum.sh /tmp/dc-$(hostname)/sample %s /tmp/results/time_$(hostname)_end" % (
            self.configs['exp_env']['convergence_checksum'])
        execute_cmd(cmd, hosts_dest, mode='start')

        logger.info("----Copying file to elmerfs mount point on source host")
        cmd = "bash /tmp/convergence_files/timing_copy_file.sh /tmp/convergence_files/sample /tmp/dc-$(hostname)/sample /tmp/results/time_$(hostname)_start"
        execute_cmd(cmd, host_src)

        logger.info("----Waiting for checksum process on all destination hosts complete")
        checksum_ok = False
        for i in range(100):
            sleep(15)
            cmd = "ps aux | grep periodically_checksum | grep sample | awk '{print$2}'"
            _, r = execute_cmd(cmd, hosts_dest)
            for p in r.processes:
                if len(p.stdout.strip().split('\n')) > 1:
                    break
            else:
                checksum_ok = True
                break
        if not checksum_ok:
            cmd = "pkill -f periodically_checksum"
            execute_cmd(cmd, hosts_dest)

    def iobench(self, elmerfs_hosts):
        pass

    def contentions(self, elmerfs_hosts):
        pass

    def run_benchmark(self, comb, elmerfs_hosts, antidote_dc):
        benchmarks = comb['benchmarks']
        logger.info("--Running benchmark %s" % benchmarks)
        if benchmarks == "convergence":
            self.convergence(comb, antidote_dc)
        if benchmarks == "performances":
            self.iobench(elmerfs_hosts, antidote_dc)
        if benchmarks == "contentions":
            self.contentions(elmerfs_hosts, antidote_dc)

    def save_results(self, comb, hosts):
        logger.info("--Starting dowloading the results")

        get_results(comb=comb,
                    hosts=hosts,
                    remote_result_files=['/tmp/results/*'],
                    local_result_dir=self.configs['exp_env']['results_dir'])

    def run_workflow(self, elmerfs_hosts, antidote_dc, comb, sweeper):
        comb_ok = False

        try:
            logger.info("=======================================")
            logger.info("Performing combination: " + slugify(comb))

            self.clean_exp_env(elmerfs_hosts)
            self.set_latency(comb["latency"], antidote_dc)
            self.run_benchmark(comb, elmerfs_hosts, antidote_dc)
            self.reset_latency(antidote_dc)
            self.save_results(comb, elmerfs_hosts)
            comb_ok = True
        except ExecuteCommandException as e:
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

    def install_benchsoftware(self, hosts):
        logger.info("Starting installing benchmark software")

        logger.info("Installing tccommand")
        cmd = "curl -sSL https://raw.githubusercontent.com/thombashi/tcconfig/master/scripts/installer.sh | bash"
        execute_cmd(cmd, hosts)

        configurator = packages_configurator()
        configurator.install_packages(["bonnie++"], hosts)

        logger.info("Installing crefi")
        cmd = "pip install pyxattr"
        execute_cmd(cmd, hosts)
        cmd = "pip install crefi"
        execute_cmd(cmd, hosts)

        # prepare data on all host for convergence experiment
        cmd = "mkdir -p /tmp/results"
        execute_cmd(cmd, hosts)
        cmd = "mkdir -p /tmp/convergence_files"
        execute_cmd(cmd, hosts)
        cmd = "wget https://raw.githubusercontent.com/ntlinh16/elmerfs-eval/main/convergence_exp_resources/timing_copy_file.sh -P /tmp/convergence_files/"
        execute_cmd(cmd, hosts)
        cmd = "wget https://raw.githubusercontent.com/ntlinh16/elmerfs-eval/main/convergence_exp_resources/periodically_checksum.sh -P /tmp/convergence_files/"
        execute_cmd(cmd, hosts)
        test_file = self.configs['exp_env']['convergence_test_file']
        cmd = "wget %s -P /tmp/convergence_files/" % test_file
        execute_cmd(cmd, hosts)
        cmd = "mv /tmp/convergence_files/%s /tmp/convergence_files/sample" % (
            test_file.split('/')[-1])
        execute_cmd(cmd, hosts)
        logger.info("Finish installing benchmark software on hosts\n")

    def deploy_elmerfs(self, kube_master, kube_namespace, elmerfs_hosts):
        logger.info("Starting deploying elmerfs on hosts")

        configurator = packages_configurator()
        configurator.install_packages(["libfuse2", "wget", "jq"], elmerfs_hosts)

        elmerfs_repo = self.configs["exp_env"]["elmerfs_repo"]
        elmerfs_version = self.configs["exp_env"]["elmerfs_version"]
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
                cmd = "umount /tmp/dc-$(hostname)"
                execute_cmd(cmd, host)
                cmd = "rm -rf /tmp/dc-$(hostname)"
                execute_cmd(cmd, host)

        logger.info("Delete elmerfs project folder on host (if existing)")
        cmd = "rm -rf /tmp/elmerfs_repo"
        execute_cmd(cmd, kube_master)

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

        logger.debug("Getting IP of antidoteDB instances on nodes")
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

        for host in elmerfs_hosts:
            antidote_options = ["--antidote=%s:8087" % ip for ip in antidote_ips[host]]

            elmerfs_cmd = "RUST_BACKTRACE=1 RUST_LOG=debug nohup /tmp/elmerfs %s --mount=/tmp/dc-$(hostname) --no-locks > /tmp/elmer.log 2>&1" % " ".join(
                antidote_options)
            logger.info("Starting elmerfs on %s with cmd: %s" % (host, elmerfs_cmd))
            execute_cmd(elmerfs_cmd, host, mode='start')
            sleep(10)

            for i in range(10):
                cmd = "pidof elmerfs"
                _, r = execute_cmd(cmd, host)
                pid = r.processes[0].stdout.strip().split(" ")

                if len(pid) >= 1 and pid[0].strip():
                    break
                else:
                    execute_cmd(elmerfs_cmd, host, mode="start")
                    sleep(10)
            else:
                logger.info("Cannot deploy elmerfs on host %s" % host)
                return False
        logger.info("Finish deploying elmerfs\n")

    def config_antidote(self, kube_namespace):
        logger.info("Starting deploying Antidote cluster")
        antidote_k8s_dir = self.configs["exp_env"]["antidote_yaml_path"]

        logger.info("Deleting all k8s resource in namespace %s" % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

        logger.debug("Delete old createDC, connectDCs_antidote and exposer-service files if exists")
        for filename in os.listdir(antidote_k8s_dir):
            if (
                filename.startswith("createDC_")
                or filename.startswith("statefulSet_")
                or filename.startswith("exposer-service_")
                or filename.startswith("connectDCs_antidote")
            ):
                if ".template" not in filename:
                    try:
                        os.remove(os.path.join(antidote_k8s_dir, filename))
                    except OSError:
                        logger.debug("Error while deleting file")

        logger.debug("Modify the statefulSet file")
        file_path = os.path.join(antidote_k8s_dir, "statefulSet.yaml.template")
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        statefulSet_files = [os.path.join(antidote_k8s_dir, "headlessService.yaml")]
        for cluster in self.configs["exp_env"]["antidote_clusters"]:
            doc["spec"]["replicas"] = self.configs["exp_env"]["n_antidotedb_per_dc"]
            doc["metadata"]["name"] = "antidote-%s" % cluster
            doc["spec"]["template"]["spec"]["nodeSelector"] = {
                "service_g5k": "antidote",
                "cluster_g5k": "%s" % cluster,
            }
            file_path = os.path.join(antidote_k8s_dir, "statefulSet_%s.yaml" % cluster)
            with open(file_path, "w") as f:
                yaml.safe_dump(doc, f)
            statefulSet_files.append(file_path)

        logger.info("Starting AntidoteDB instances")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        configurator.deploy_k8s_resources(files=statefulSet_files, namespace=kube_namespace)

        logger.info("Waiting until all Antidote instances are up")
        configurator.wait_k8s_resources(resource="pod",
                                        label_selectors="app=antidote",
                                        kube_namespace=kube_namespace,)

        logger.debug("Creating createDc.yaml file for each Antidote DC")
        antidote_dc = dict()
        for cluster in self.configs["exp_env"]["antidote_clusters"]:
            antidote_dc[cluster] = dict()
            antidote_dc[cluster]['host_names'] = list()
            antidote_dc[cluster]['pod_names'] = list()
            antidote_dc[cluster]['pod_ips'] = list()

        antidote_pods = configurator.get_k8s_resources(resource="pod",
                                                       label_selectors="app=antidote",
                                                       kube_namespace=kube_namespace,)
        for pod in antidote_pods.items:
            cluster = pod.spec.node_name.split("-")[0].strip()
            if pod.spec.node_name not in antidote_dc[cluster]['host_names']:
                antidote_dc[cluster]['host_names'].append(pod.spec.node_name)
            antidote_dc[cluster]['pod_names'].append(pod.metadata.name)
            antidote_dc[cluster]['pod_ips'].append(pod.status.pod_ip)

        file_path = os.path.join(antidote_k8s_dir, "createDC.yaml.template")
        with open(file_path) as f:
            doc = yaml.safe_load(f)

        antidote_masters = list()
        createdc_files = list()
        for cluster, cluster_info in antidote_dc.items():
            doc["spec"]["template"]["spec"]["containers"][0]["args"] = [
                "--createDc", "%s.antidote:8087" % cluster_info['pod_names'][0]
            ] + ["antidote@%s.antidote" % pod for pod in cluster_info['pod_names']]
            doc["metadata"]["name"] = "createdc-%s" % cluster
            antidote_masters.append("%s.antidote:8087" % cluster_info['pod_names'][0])
            file_path = os.path.join(antidote_k8s_dir, "createDC_%s.yaml" % cluster)
            with open(file_path, "w") as f:
                yaml.safe_dump(doc, f)
            createdc_files.append(file_path)

        logger.debug("Creating exposer-service.yaml files")
        file_path = os.path.join(antidote_k8s_dir, "exposer-service.yaml.template")
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster, cluster_info in antidote_dc.items():
            doc["spec"]["selector"]["statefulset.kubernetes.io/pod-name"] = cluster_info['pod_names'][0]
            doc["metadata"]["name"] = "antidote-exposer-%s" % cluster
            file_path = os.path.join(antidote_k8s_dir, "exposer-service_%s.yaml" % cluster)
            with open(file_path, "w") as f:
                yaml.safe_dump(doc, f)
                createdc_files.append(file_path)

        logger.info("Creating Antidote DCs and exposing services")
        configurator.deploy_k8s_resources(files=createdc_files, namespace=kube_namespace)

        logger.info("Waiting until all antidote DCs are created")
        configurator.wait_k8s_resources(resource="job",
                                        label_selectors="app=antidote",
                                        kube_namespace=kube_namespace,)

        logger.debug("Creating connectDCs_antidote.yaml to connect all Antidote DCs")
        file_path = os.path.join(antidote_k8s_dir, "connectDCs.yaml.template")
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        doc["spec"]["template"]["spec"]["containers"][0]["args"] = [
            "--connectDcs"] + antidote_masters
        file_path = os.path.join(antidote_k8s_dir, "connectDCs_antidote.yaml")
        with open(file_path, "w") as f:
            yaml.safe_dump(doc, f)

        logger.info("Connecting all Antidote DCs into a cluster")
        configurator.deploy_k8s_resources(files=[file_path], namespace=kube_namespace)

        logger.info("Waiting until connecting all Antidote DCs")
        configurator.wait_k8s_resources(resource="job",
                                        label_selectors="app=antidote",
                                        kube_namespace=kube_namespace,)
        logger.info("Finish deploying the Antidote cluster\n")
        return antidote_dc

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

    def config_host(self, kube_master_site, kube_namespace):
        kube_master = self.args.kube_master

        if self.args.kube_master is None:
            antidote_hosts = list()
            for cluster in self.configs["exp_env"]["antidote_clusters"]:
                if cluster == self.configs["exp_env"]["kube_master_site"]:
                    antidote_hosts += [
                        host for host in self.hosts if host.startswith(cluster)
                    ][0: self.configs["exp_env"]["n_antidotedb_per_dc"] + 1]
                else:
                    antidote_hosts += [
                        host for host in self.hosts if host.startswith(cluster)
                    ][0: self.configs["exp_env"]["n_antidotedb_per_dc"]]

            for host in antidote_hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break
            elmerfs_hosts = antidote_hosts
            elmerfs_hosts.remove(kube_master)

            self.config_kube(kube_master, antidote_hosts, kube_namespace)
        else:
            logger.info("Kubernetes master: %s" % kube_master)
            self._get_credential(kube_master)

            configurator = k8s_resources_configurator()
            antidote_hosts = configurator.get_k8s_resources_name(resource="node",
                                                                 label_selectors="service_g5k=antidote")

            elmerfs_hosts = antidote_hosts

        antidote_dc = self.config_antidote(kube_namespace)

        logger.info("antidote_dc = %s" % antidote_dc)

        self.deploy_elmerfs(kube_master, kube_namespace, elmerfs_hosts)
        self.install_benchsoftware(elmerfs_hosts)
        if self.args.monitoring:
            self.deploy_monitoring(kube_master, kube_namespace)

        return antidote_dc, elmerfs_hosts

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

        antidote_dc, elmerfs_hosts = self.config_host(kube_master, kube_namespace)

        logger.info("Finish configuring nodes\n")

        self.args.oar_job_ids = None
        logger.info("Finish configuring the experiment environment\n")
        return oar_job_ids, antidote_dc, elmerfs_hosts

    def create_configs(self):
        logger.debug("Get the k8s master node")
        kube_master_site = self.configs["exp_env"]["kube_master_site"]
        if (
            kube_master_site is None
            or kube_master_site not in self.configs["exp_env"]["antidote_clusters"]
        ):
            kube_master_site = self.configs["exp_env"]["antidote_clusters"][0]

        # calculating the total number of hosts for each cluster
        clusters = dict()
        for cluster in self.configs["exp_env"]["antidote_clusters"]:
            if cluster == kube_master_site:
                clusters[cluster] = (
                    clusters.get(cluster, 0)
                    + self.configs["exp_env"]["n_antidotedb_per_dc"]
                    + 1
                )
            else:
                clusters[cluster] = (
                    clusters.get(cluster, 0)
                    + self.configs["exp_env"]["n_antidotedb_per_dc"]
                )

        self.configs["clusters"] = [
            {"cluster": cluster, "n_nodes": n_nodes}
            for cluster, n_nodes in clusters.items()
        ]

        return kube_master_site

    def run(self):
        logger.debug("Parse and convert configs for G5K provisioner")
        self.configs = parse_config_file(self.args.config_file_path)
        kube_master_site = self.create_configs()

        logger.info(
            """Your topology:
                        Antidote DCs: %s
                        n_antidotedb_per_DC: %s """
            % (len(self.configs["exp_env"]["antidote_clusters"]),
                self.configs["exp_env"]["n_antidotedb_per_dc"],)
        )

        # Logarithmic scale interval of latency
        if self.configs["parameters"]["latency_interval"] == "logarithmic scale":
            start, end = self.configs["parameters"]["latency"]
            latency = [start, end]
            log_start = int(math.ceil(math.log(start)))
            log_end = int(math.ceil(math.log(end)))
            for i in range(log_start, log_end):
                latency.append(int(math.exp(i)))
                latency.append(int(math.exp(i + 0.5)))
            del self.configs["parameters"]["latency_interval"]
            self.configs["parameters"]["latency"] = list(set(latency))

        sweeper = create_combs_queue(result_dir=self.configs["exp_env"]["results_dir"],
                                     parameters=self.configs["parameters"],)

        kube_namespace = "elmerfs-exp"
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                oar_job_ids, antidote_dc, elmerfs_hosts = self.setup_env(
                    kube_master_site, kube_namespace)

            comb = sweeper.get_next()
            sweeper = self.run_workflow(elmerfs_hosts=elmerfs_hosts,
                                        antidote_dc=antidote_dc,
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
