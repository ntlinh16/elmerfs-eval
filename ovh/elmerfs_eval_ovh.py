import os
import shutil
import traceback
import random

from cloudal.action import performing_actions
from cloudal.configurator import (kubernetes_configurator, 
                                  k8s_resources_configurator, 
                                  antidotedb_configurator, 
                                  packages_configurator,
                                  elmerfs_configurator,
                                  filebench_configurator, 
                                  CancelException)
from cloudal.experimenter import (create_paramsweeper, 
                                  define_parameters, 
                                  get_results, 
                                  delete_ovh_nodes, 
                                  is_node_active)
from cloudal.provisioner import ovh_provisioner
from cloudal.utils import (get_logger, 
                           execute_cmd, 
                           parse_config_file, 
                           getput_file, 
                           ExecuteCommandException)

from execo_engine import slugify
from kubernetes import config


logger = get_logger()

class elmerfs_eval_ovh(performing_actions):
    def __init__(self):
        super(elmerfs_eval_ovh, self).__init__()
        self.args_parser.add_argument('--node_ids_file', dest='node_ids_file',
                                      help='the path to the file contents list of node IDs',
                                      default=None,
                                      type=str)
        self.args_parser.add_argument('--kube_master', dest='kube_master',
                                      help='name of kube master node',
                                      default=None,
                                      type=str)
        self.args_parser.add_argument('--setup-k8s-env', dest='setup_k8s_env',
                                      help='create namespace, setup label and volume for kube_workers for the experiment environment',
                                      action='store_true')
        self.args_parser.add_argument('--monitoring', dest='monitoring',
                                      help='deploy Grafana and Prometheus for monitoring',
                                      action='store_true')
        self.args_parser.add_argument('--attach_volume', dest='attach_volume',
                                      help='attach an external volume to every data node',
                                      action='store_true')
        self.args_parser.add_argument("--no_config_host", dest="no_config_host",
                                      help="do not run the functions to config the hosts",
                                      action="store_true")
        self.args_parser.add_argument("-k", dest="keep_nodes",
                                      help="keep the provisioned nodes after finishing the experiments",
                                      action="store_true")

    def save_results(self, comb, hosts):
        logger.info('----------------------------------')
        logger.info('5. Starting dowloading the results')

        get_results(comb=comb,
                               hosts=hosts,
                               remote_result_files=['/tmp/results/*'],
                               local_result_dir=self.configs['exp_env']['results_dir'])

    def run_benchmark(self, comb, elmerfs_hosts, elmerfs_mountpoint):
        benchmarks = comb['benchmarks']
        logger.info('-----------------------------------')
        logger.info('4. Running benchmark %s' % benchmarks)
        filebench_hosts = random.sample(elmerfs_hosts, comb['n_clients'])
        elmerfs_mountpoint='tmp\/dc-$(hostname)'
        configurator = filebench_configurator()
        if benchmarks == 'mailserver':
            is_finished = configurator.run_mailserver(filebench_hosts, elmerfs_mountpoint, comb['duration'], comb['n_threads'])
            return is_finished, filebench_hosts

    def deploy_elmerfs(self, kube_master, kube_namespace, elmerfs_hosts, elmerfs_mountpoint, antidote_ips):
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

    def clean_exp_env(self, kube_namespace):
        logger.info('1. Cleaning the experiment environment')
        logger.info('Delete running filebench process on elmerfs nodes')
        cmd = 'pkill -f filebench'
        execute_cmd(cmd, self.hosts)

        logger.info('Deleting all k8s running resources from the previous run in namespace "%s"' % kube_namespace)
        logger.debug('Delete namespace "%s" to delete all the running resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)



    def run_exp_workflow(self, kube_namespace, comb, kube_master, sweeper, elmerfs_mountpoint):
        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(kube_namespace)
            self.deploy_antidote(kube_namespace, comb)

            logger.debug('Getting hosts and IP of antidoteDB instances on their nodes')
            antidote_ips = dict()
            antidote_hosts = list()
            configurator = k8s_resources_configurator()
            pod_list = configurator.get_k8s_resources(resource='pod',
                                                      label_selectors='app=antidote',
                                                      kube_namespace=kube_namespace,)
            for pod in pod_list.items:
                node_ip = pod.status.host_ip
                if node_ip not in antidote_ips:
                    antidote_ips[node_ip] = list()
                antidote_ips[node_ip].append(pod.status.pod_ip)
            antidote_hosts = list(antidote_ips.keys())
            elmerfs_hosts = antidote_hosts
            
            is_elmerfs = self.deploy_elmerfs(kube_master, kube_namespace, elmerfs_hosts, elmerfs_mountpoint, antidote_ips)
            if is_elmerfs:
                if self.args.monitoring:
                    self.deploy_monitoring(kube_master, kube_namespace)
                is_finished, hosts = self.run_benchmark(comb, elmerfs_hosts, elmerfs_mountpoint)
                if is_finished:
                    comb_ok = True
                    self.save_results(comb, hosts)
            else:
                raise CancelException('Cannot deploy elmerfs')
        except (ExecuteCommandException, CancelException) as e:
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

    def _setup_ovh_kube_volumes(self, kube_workers, n_pv=3):
        logger.info('Setting volumes on %s kubernetes workers' % len(kube_workers))
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
        logger.info('Creating local persistance volumes on Kubernetes cluster')
        logger.debug('Init configurator: k8s_resources_configurator')
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs['exp_env']['antidotedb_yaml_path']
        deploy_files = [os.path.join(antidote_k8s_dir, 'local_persistentvolume.yaml'),
                        os.path.join(antidote_k8s_dir, 'storageClass.yaml')]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info('Waiting for setting local persistance volumes')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors='app.kubernetes.io/instance=local-volume-provisioner')

    def _set_kube_workers_label(self, kube_master):
        configurator = k8s_resources_configurator()
        for node in self.nodes:
            if node['ipAddresses'][0]['ip'] == kube_master:
                pass
            else:
                r = configurator.set_labels_node(nodename=node['name'],
                                                 labels='cluster=%s,service=antidote' % node['region'].lower())
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
        logger.debug('Init configurator: kubernetes_configurator')
        configurator = kubernetes_configurator(hosts=self.hosts, kube_master=kube_master)
        _, kube_workers = configurator.deploy_kubernetes_cluster()

        return kube_workers

    def setup_k8s_env(self, kube_master, kube_namespace, kube_workers):
        self._get_credential(kube_master)

        logger.info('Create k8s namespace "%s" for running resources of this experiment' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.create_namespace(namespace=kube_namespace)

        logger.info('Set labels for all kubernetes workers')
        self._set_kube_workers_label(kube_master)

        self._setup_ovh_kube_volumes(kube_workers, n_pv=3)

        logger.info('Finish deploying the Kubernetes cluster\n')

    def config_host(self, kube_master, kube_namespace, elmerfs_mountpoint):
        logger.info('Starting configuring nodes')

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

        if not self.args.no_config_host:
            logger.info("Installing tcconfig")
            configurator = packages_configurator()
            configurator.install_packages(['python3-pip'], self.hosts)  
            cmd = "pip3 install tcconfig"
            execute_cmd(cmd, self.hosts)

            configurator = elmerfs_configurator()
            configurator.install_elmerfs(kube_master=kube_master,
                                         elmerfs_hosts=kube_workers,
                                         elmerfs_mountpoint=elmerfs_mountpoint,
                                         elmerfs_repo=self.configs['exp_env']['elmerfs_repo'],
                                         elmerfs_version=self.configs['exp_env']['elmerfs_version'],
                                         elmerfs_path=self.configs['exp_env']['elmerfs_path'])

            # Installing benchmark for running the experiments
            configurator = filebench_configurator()
            configurator.install_filebench(kube_workers)

        logger.info('Finish configuring nodes')
        return kube_master

    def setup_env(self, kube_master_site, kube_namespace, elmerfs_mountpoint):
        logger.info('STARTING SETTING THE EXPERIMENT ENVIRONMENT')
        logger.info('Starting provisioning nodes on OVHCloud')

        provisioner = ovh_provisioner(configs=self.configs, node_ids_file=self.args.node_ids_file)
        provisioner.provisioning()

        self.nodes = provisioner.nodes
        self.hosts = provisioner.hosts
        node_ids = provisioner.node_ids
        driver = provisioner.driver

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
            data_nodes += nodes[0: max(self.normalized_parameters['n_nodes_per_dc'])]
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

        self.config_host(kube_master, kube_namespace, elmerfs_mountpoint)

        logger.info('FINISH SETTING THE EXPERIMENT ENVIRONMENT\n')
        return kube_master, node_ids, driver

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = max(self.normalized_parameters['n_nodes_per_dc'])

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
            max(self.normalized_parameters['n_nodes_per_dc'])
        )
        )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)
        return sweeper, kube_master_site


    def run(self):
        sweeper, kube_master_site = self.create_combination_queue()

        kube_namespace = 'elmerfs-exp'
        elmerfs_mountpoint = '/tmp/dc-$(hostname)'
        node_ids = None
        while len(sweeper.get_remaining()) > 0:
            if node_ids is None:
                kube_master, node_ids, driver = self.setup_env(kube_master_site, kube_namespace, elmerfs_mountpoint)
            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(kube_namespace=kube_namespace,
                                            kube_master=kube_master,
                                            comb=comb,
                                            sweeper=sweeper,
                                            elmerfs_mountpoint=elmerfs_mountpoint)

            logger.info('==================================================')
            logger.info('Checking whether all provisioned nodes are running')
            is_nodes_alive, _ = is_node_active(node_ids=node_ids, 
                                               project_id=self.configs['project_id'], 
                                               driver=driver)
            if not is_nodes_alive:
                logger.info('Deleting old provisioned nodes')
                delete_ovh_nodes(node_ids=node_ids,
                                project_id=self.configs['project_id'], 
                                driver=driver)
                node_ids = None
        logger.info('Finish the experiment!!!')
        logger.info('The provisioned nodes keep running')
        if not self.args.keep_nodes:
            logger.info('Deleting provisioned nodes after finishing the experiment')
            delete_ovh_nodes(node_ids=node_ids,
                            project_id=self.configs['project_id'], 
                            driver=driver)


if __name__ == '__main__':
    logger.info('Init engine in %s' % __file__)
    engine = elmerfs_eval_ovh()

    try:
        logger.info('Start engine in %s' % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')
