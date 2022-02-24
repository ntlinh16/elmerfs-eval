from logging import raiseExceptions
import os
from re import L
import shutil
import traceback
import random
import math

from cloudal.action import performing_actions_g5k
from cloudal.configurator import (kubernetes_configurator, 
                                  k8s_resources_configurator, 
                                  antidotedb_configurator,
                                  elmerfs_configurator, 
                                  filebench_configurator,
                                  CancelException)
from cloudal.experimenter import (create_paramsweeper,
                                  is_job_alive, 
                                  get_results, 
                                  define_parameters)
from cloudal.provisioner import g5k_provisioner
from cloudal.utils import (get_logger,
                           execute_cmd,
                           parse_config_file,
                           getput_file,
                           ExecuteCommandException,)

from execo_g5k import oardel
from execo_engine import slugify
from kubernetes import config

logger = get_logger()


class elmerfs_eval_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(elmerfs_eval_g5k, self).__init__()
        self.args_parser.add_argument('--kube_master',
                                      dest='kube_master',
                                      help='name of kube master node',
                                      default=None,
                                      type=str,)
        self.args_parser.add_argument('--monitoring', 
                                      dest='monitoring',
                                      help='deploy Grafana and Prometheus for monitoring',
                                      action='store_true')
        self.args_parser.add_argument("--no_config_host", dest="no_config_host",
                                      help="do not run the functions to config the hosts",
                                      action="store_true")

    def save_results(self, comb, nodes):
        logger.info('----------------------------------')
        logger.info('5. Starting downloading the results')

        get_results(comb=comb,
                    hosts=nodes,
                    remote_result_files=['/tmp/results/*'],
                    local_result_dir=self.configs['exp_env']['results_dir'])

    def run_benchmark(self, comb, elmerfs_hosts, elmerfs_mountpoint):
        benchmark = comb['benchmarks']
        logger.info('-----------------------------------')
        logger.info('4. Running benchmark %s' % benchmark)
        filebench_hosts = random.sample(elmerfs_hosts, comb['n_clients'])
        elmerfs_mountpoint='tmp\/dc-$(hostname)'
        configurator = filebench_configurator()
        if benchmark == 'mailserver':
            is_finished = configurator.run_mailserver(filebench_hosts, elmerfs_mountpoint, comb['duration'], comb['n_threads'])
            return is_finished, filebench_hosts

    def deploy_elmerfs(self, kube_namespace, elmerfs_hosts, elmerfs_mountpoint, antidote_ips):
        logger.info('-----------------------------------------')
        logger.info('3. Starting deploying elmerfs on %s hosts' % len(elmerfs_hosts))
        configurator = elmerfs_configurator()
        is_deploy = configurator.deploy_elmerfs(clusters=self.configs['exp_env']['clusters'],
                                                kube_namespace=kube_namespace,
                                                antidote_ips=antidote_ips,
                                                elmerfs_hosts=elmerfs_hosts,
                                                elmerfs_mountpoint=elmerfs_mountpoint)
        return is_deploy

    def deploy_monitoring(self, kube_master, kube_namespace):
        logger.info('--------------------------------------')
        logger.info('Deploying monitoring system')
        configurator = antidotedb_configurator()
        prometheus_url, grafana_url = configurator.deploy_monitoring(node=kube_master,
                                                                     monitoring_yaml_path = self.configs['exp_env']['monitoring_yaml_path'], 
                                                                     kube_namespace=kube_namespace)
        return prometheus_url, grafana_url
    
    def deploy_antidote(self, kube_namespace, comb):
        logger.info('--------------------------------------')
        logger.info('2. Starting deploying Antidote cluster')
        configurator = antidotedb_configurator()
        configurator.deploy_antidotedb(n_nodes=comb['n_nodes_per_dc'], 
                                       antidotedb_yaml_path=self.configs['exp_env']['antidotedb_yaml_path'], 
                                       clusters=self.configs['exp_env']['clusters'], 
                                       kube_namespace=kube_namespace)

    def get_latency(self, hostA, hostB):
        cmd = "ping -c 4 %s" % hostB
        _, r = execute_cmd(cmd, hostA)
        tokens = r.processes[0].stdout.strip().split('\r\n')[3].split('time=')
        logger.info('tokens = %s' % tokens)
        if len(tokens) == 2:
            return  math.ceil(float(tokens[1].split()[0]))
        raise CancelException("Cannot get latency between nodes")
    
    def reset_latency(self):
        """Delete the Limitation of latency in host"""
        logger.info("--Remove network latency on hosts")
        cmd = "tcdel flannel.1 --all"
        execute_cmd(cmd, self.hosts)

    def set_latency(self, latency, kube_namespace):
        """Limit the latency in host"""
        logger.info('---------------------------------------')
        logger.info('Setting network latency = %s on hosts' % latency)
        antidote_dc = dict()
        for cluster in self.configs["exp_env"]["clusters"]:
            antidote_dc[cluster] = dict()
            antidote_dc[cluster]['host_names'] = list()
            antidote_dc[cluster]['pod_names'] = list()
            antidote_dc[cluster]['pod_ips'] = list()
        configurator = k8s_resources_configurator()
        antidote_pods = configurator.get_k8s_resources(resource="pod",
                                                       label_selectors="app=antidote",
                                                       kube_namespace=kube_namespace,)
        for pod in antidote_pods.items:
            cluster = pod.spec.node_name.split("-")[0].strip()
            if pod.spec.node_name not in antidote_dc[cluster]['host_names']:
                antidote_dc[cluster]['host_names'].append(pod.spec.node_name)
            antidote_dc[cluster]['pod_names'].append(pod.metadata.name)
            antidote_dc[cluster]['pod_ips'].append(pod.status.pod_ip)

        logger.info('antidote_dc = %s' % antidote_dc)

        for cur_cluster, cur_cluster_info in antidote_dc.items():
            other_clusters = {cluster_name: cluster_info
                              for cluster_name, cluster_info in antidote_dc.items() if cluster_name != cur_cluster}

            for other_cluster, cluster_info in other_clusters.items():
                real_latency = self.get_latency(cur_cluster_info['host_names'][0], cluster_info['host_names'][0])
                logger.info('latency between %s and %s is: %s' % (cur_cluster, other_cluster, real_latency))
                if real_latency < latency:
                    latency_add = (latency - real_latency)/2
                    logger.info('Add %s to current latency from %s cluster to %s:' % (latency_add, cur_cluster, other_cluster))
                else:
                    self.reset_latency()
                    return False
                for pod_ip in cluster_info['pod_ips']:
                    cmd = "tcset flannel.1 --delay %s --network %s" % (latency_add, pod_ip)
                    logger.info('%s --->  %s, cmd = %s' % (cur_cluster_info['host_names'], pod_ip, cmd))
                    execute_cmd(cmd, cur_cluster_info['host_names'])
        return True
    
    def clean_exp_env(self, kube_namespace):
        logger.info('1. Cleaning the experiment environment')
        logger.info('Deleting all k8s running resources from the previous run in namespace "%s"' % kube_namespace)
        logger.debug('Delete namespace "%s" to delete all the running resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)


    def run_workflow(self, kube_namespace, kube_master, comb, sweeper, elmerfs_mountpoint):

        comb_ok = ''

        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(kube_namespace)
            self.deploy_antidote(kube_namespace, comb)

            logger.debug('Getting hosts and IP of antidoteDB instances on their nodes')
            antidote_ips = dict()
            configurator = k8s_resources_configurator()
            pod_list = configurator.get_k8s_resources(resource='pod',
                                                      label_selectors='app=antidote',
                                                      kube_namespace=kube_namespace)
            for pod in pod_list.items:
                node_ip = pod.status.host_ip
                if node_ip not in antidote_ips:
                    antidote_ips[node_ip] = list()
                antidote_ips[node_ip].append(pod.status.pod_ip)
            antidote_hosts = list(antidote_ips.keys())
            elmerfs_hosts = antidote_hosts

            is_elmerfs = self.deploy_elmerfs(kube_namespace, elmerfs_hosts, elmerfs_mountpoint, antidote_ips)
            if is_elmerfs:
                if self.args.monitoring:
                    self.deploy_monitoring(kube_master, kube_namespace)
                if len(self.configs['exp_env']['clusters']) > 1:
                    is_latency = self.set_latency(comb["latency"], kube_namespace)
                    if not is_latency:
                        comb_ok = 'skip'
                        return sweeper                      
                is_finished, hosts = self.run_benchmark(comb, elmerfs_hosts, elmerfs_mountpoint)
                if len(self.configs['exp_env']['clusters']) > 1:
                    self.reset_latency()
                if is_finished:
                    comb_ok = 'done'
                    self.save_results(comb, hosts)
            else:
                raise CancelException('Cannot deploy elmerfs')
        except (ExecuteCommandException, CancelException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = 'cancel'
        finally:
            if comb_ok == 'done':
                sweeper.done(comb)
                logger.info('Finish combination: %s' % slugify(comb))
            elif comb_ok == 'cancel':
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' is canceled')
            else:
                sweeper.skip(comb)
                logger.warning(slugify(comb) + ' is skipped due to real_latency is higher than %s' % comb['latency'])
            logger.info('%s combinations remaining\n' % len(sweeper.get_remaining()))
        return sweeper
 
    def _set_kube_workers_label(self, kube_workers):
        logger.info('Set labels for all kubernetes workers')
        configurator = k8s_resources_configurator()
        for host in kube_workers:
            cluster = host.split('-')[0]
            labels = 'cluster=%s,service=antidote' % cluster
            configurator.set_labels_node(host, labels)

    def _setup_g5k_kube_volumes(self, kube_workers, n_pv=3):

        logger.info('Setting volumes on %s kubernetes workers' % len(kube_workers))
        cmd = '''umount /dev/sda5;
                 mount -t ext4 /dev/sda5 /tmp'''
        execute_cmd(cmd, kube_workers)
        logger.debug('Create n_pv partitions on the physical disk to make a PV can be shared')
        cmd = '''for i in $(seq 1 %s); do
                     mkdir -p /tmp/pv/vol${i}
                     mkdir -p /mnt/disks/vol${i}
                     mount --bind /tmp/pv/vol${i} /mnt/disks/vol${i}
                 done''' % n_pv
        execute_cmd(cmd, kube_workers)

        logger.info('Creating local persistance volumes on Kubernetes cluster')
        logger.debug('Init configurator: k8s_resources_configurator')
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs['exp_env']['antidotedb_yaml_path']
        deploy_files = [
            os.path.join(antidote_k8s_dir, 'local_persistentvolume.yaml'),
            os.path.join(antidote_k8s_dir, 'storageClass.yaml'),
        ]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info('Waiting for setting local persistance volumes')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors='app.kubernetes.io/instance=local-volume-provisioner',)

    def _get_credential(self, kube_master):
        home = os.path.expanduser('~')
        kube_dir = os.path.join(home, '.kube')
        if not os.path.exists(kube_dir):
            os.mkdir(kube_dir)
        getput_file(hosts=[kube_master],
                    file_paths=['~/.kube/config'],
                    dest_location=kube_dir,
                    action='get',)
        kube_config_file = os.path.join(kube_dir, 'config')
        config.load_kube_config(config_file=kube_config_file)
        logger.info('Kubernetes config file is stored at: %s' % kube_config_file)

    def config_kube(self, kube_master, antidote_hosts, kube_namespace):
        logger.info('Starting configuring a Kubernetes cluster')
        logger.debug('Init configurator: kubernetes_configurator')
        configurator = kubernetes_configurator(hosts=self.hosts, kube_master=kube_master)
        configurator.deploy_kubernetes_cluster()

        self._get_credential(kube_master)

        logger.info('Create k8s namespace "%s" for this experiment' % kube_namespace)
        logger.debug('Init configurator: k8s_resources_configurator')
        configurator = k8s_resources_configurator()
        configurator.create_namespace(kube_namespace)

        kube_workers = [host for host in antidote_hosts if host != kube_master]

        self._setup_g5k_kube_volumes(kube_workers, n_pv=3)

        self._set_kube_workers_label(kube_workers)

        logger.info('Finish configuring the Kubernetes cluster\n')
        return kube_workers

    def config_host(self, kube_master_site, kube_namespace, elmerfs_mountpoint):
        kube_master = self.args.kube_master

        if self.args.kube_master is None:
            antidote_hosts = list()
            for cluster in self.configs['clusters']:
                cluster_name = cluster['cluster']
                if cluster_name == self.configs['exp_env']['kube_master_site']:
                    antidote_hosts += [host for host in self.hosts if host.startswith(cluster_name)][0: cluster['n_nodes'] + 1]
                else:
                    antidote_hosts += [host for host in self.hosts if host.startswith(cluster_name)][0: cluster['n_nodes']]

            for host in antidote_hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break

            kube_workers = self.config_kube(kube_master, antidote_hosts, kube_namespace)
        else:
            logger.info('Kubernetes master: %s' % kube_master)
            self._get_credential(kube_master)

            # kube_workers = [host for host in antidote_hosts if host != kube_master]
            self._setup_g5k_kube_volumes(kube_workers, n_pv=3)
            self._set_kube_workers_label(kube_workers)
        
        if not self.args.no_config_host:
            logger.info("Installing tcconfig")
            cmd = "pip3 install tcconfig"
            execute_cmd(cmd, self.hosts)
            configurator = elmerfs_configurator()
            configurator.install_elmerfs(kube_master=kube_master,
                                         elmerfs_hosts=kube_workers,
                                         elmerfs_mountpoint=elmerfs_mountpoint,
                                         elmerfs_repo=self.configs['exp_env']['elmerfs_repo'],
                                         elmerfs_version=self.configs['exp_env']['elmerfs_version'],
                                         elmerfs_path=self.configs['exp_env']['elmerfs_path'])
            configurator = filebench_configurator()
            configurator.install_filebench(kube_workers)

    def calculate_latency_range(self):
        latency_interval = self.configs["parameters"]["latency_interval"]
        start, end = self.configs["parameters"]["latency"]
        if latency_interval == "logarithmic scale":
            latency = [start, end]
            log_start = int(math.ceil(math.log(start)))
            log_end = int(math.ceil(math.log(end)))
            for i in range(log_start, log_end):
                val = int(math.exp(i))
                if val < end:
                    latency.append(int(math.exp(i)))
                val = int(math.exp(i + 0.5))
                if val < end:
                    latency.append(int(math.exp(i + 0.5)))
        elif isinstance(latency_interval, int):
            latency = [start]
            next_latency = start + latency_interval
            while next_latency < end:
                latency.append(next_latency)
                next_latency += latency_interval
            latency.append(end)
        else:
            logger.info('Please give a valid latency_interval ("logarithmic scale" or a number)')
            exit()
        del self.configs["parameters"]["latency_interval"]
        self.configs["parameters"]["latency"] = list(set(latency))
        logger.info('latency = %s' % self.configs["parameters"]["latency"])

    def setup_env(self, kube_master_site, kube_namespace, elmerfs_mountpoint):
        logger.info('Starting configuring the experiment environment')
        logger.debug('Init provisioner: g5k_provisioner')
        provisioner = g5k_provisioner(configs=self.configs,
                                      keep_alive=self.args.keep_alive,
                                      out_of_chart=self.args.out_of_chart,
                                      oar_job_ids=self.args.oar_job_ids,
                                      no_deploy_os=self.args.no_deploy_os,
                                      is_reservation=self.args.is_reservation,
                                      job_name='cloudal_elmerfs_k8s',)

        provisioner.provisioning()
        self.hosts = provisioner.hosts
        oar_job_ids = provisioner.oar_result
        self.oar_result = provisioner.oar_result

        logger.info('Starting configuring nodes')
        kube_master = self.args.kube_master
        if kube_master is None:
            for host in self.hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break

        self.config_host(kube_master, kube_namespace, elmerfs_mountpoint)

        logger.info('Finish configuring nodes\n')

        self.args.oar_job_ids = None
        logger.info('Finish configuring the experiment environment\n')
        return oar_job_ids, kube_master

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
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
        results_dir_name = (self.configs['exp_env']['results_dir']).split('/')[-1]
        results_dir_path = os.path.dirname(self.configs['exp_env']['results_dir'])

        yaml_dir_path = os.path.dirname(self.configs['exp_env']['antidotedb_yaml_path'])
        yaml_dir_name = yaml_dir_path.split('/')[-1]

        new_yaml_dir_name = yaml_dir_name + '_' + results_dir_name
        new_path = results_dir_path + '/' + new_yaml_dir_name
        if os.path.exists(new_path):
            shutil.rmtree(new_path)
        shutil.copytree(yaml_dir_path, new_path)

        self.configs['exp_env']['antidotedb_yaml_path'] = new_path + '/antidotedb_yaml'
        self.configs['exp_env']['monitoring_yaml_path'] = new_path + '/monitoring_yaml'
        return kube_master_site

    def create_combination_queue(self):
        logger.debug('Parse and convert configs for G5K provisioner')
        self.configs = parse_config_file(self.args.config_file_path)

        # Add the number of Antidote DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        if len(self.configs['exp_env']['clusters']) > 1:
            self.calculate_latency_range()

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        kube_master_site = self.create_configs()

        logger.info('''Your largest topology:
                        Antidote DCs: %s
                        n_antidotedb_per_DC: %s  ''' % (len(self.configs['exp_env']['clusters']),
                                                        max(self.normalized_parameters['n_nodes_per_dc']))
                    )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)
        return sweeper, kube_master_site

    def run(self):
        sweeper, kube_master_site = self.create_combination_queue()

        kube_namespace = 'elmerfs-exp'
        elmerfs_mountpoint = '/tmp/dc-$(hostname)'
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                oar_job_ids, kube_master = self.setup_env(kube_master_site, kube_namespace, elmerfs_mountpoint)

            comb = sweeper.get_next()
            sweeper = self.run_workflow(kube_master=kube_master,
                                        kube_namespace=kube_namespace,
                                        comb=comb,
                                        sweeper=sweeper,
                                        elmerfs_mountpoint = elmerfs_mountpoint)

            if not is_job_alive(oar_job_ids):
                oardel(oar_job_ids)
                oar_job_ids = None
        logger.info('Finish the experiment!!!')


if __name__ == '__main__':
    logger.info('Init engine in %s' % __file__)
    engine = elmerfs_eval_g5k()

    try:
        logger.info('Start engine in %s' % __file__)
        engine.start()
    except Exception as e:
        logger.error('Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')

    if not engine.args.keep_alive:
        logger.info('Deleting reservation')
        oardel(engine.oar_result)
        logger.info('Reservation deleted')
    else:
        logger.info('Reserved nodes are kept alive for inspection purpose.')
