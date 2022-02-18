from logging import raiseExceptions
import os
import shutil
import traceback
import random

from cloudal.action import performing_actions_g5k
from cloudal.configurator import (kubernetes_configurator, 
                                  k8s_resources_configurator, 
                                  antidotedb_configurator, 
                                  packages_configurator,
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

    def deploy_elmerfs(self, kube_master, kube_namespace, elmerfs_hosts, elmerfs_mountpoint, antidote_ips):
        configurator = elmerfs_configurator()
        is_deploy = configurator.deploy_elmerfs(kube_master=kube_master,
                                                clusters=self.configs['exp_env']['clusters'],
                                                kube_namespace=kube_namespace,
                                                antidote_ips=antidote_ips,
                                                elmerfs_hosts=elmerfs_hosts,
                                                elmerfs_mountpoint=elmerfs_mountpoint,
                                                elmerfs_repo=self.configs['exp_env']['elmerfs_repo'],
                                                elmerfs_version=self.configs['exp_env']['elmerfs_version'],
                                                elmerfs_path=self.configs['exp_env']['elmerfs_path'])
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
        logger.info('Deleting all k8s running resources from the previous run in namespace "%s"' % kube_namespace)
        logger.debug('Delete namespace "%s" to delete all the running resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

    def run_workflow(self, kube_namespace, kube_master, comb, sweeper, elmerfs_mountpoint):

        comb_ok = False

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

            configurator = k8s_resources_configurator()
            deployed_hosts = configurator.get_k8s_resources(resource='node')
            kube_workers = [host.metadata.name for host in deployed_hosts.items]
            kube_workers.remove(kube_master)
        
        if not self.args.no_config_host:
            logger.info('Installing elmerfs dependencies')
            configurator = packages_configurator()
            configurator.install_packages(['libfuse2', 'wget', 'jq'], kube_workers)
            # Create mount point on elmerfs hosts
            cmd = 'mkdir -p %s' % elmerfs_mountpoint
            execute_cmd(cmd, kube_workers)

            # Installing filebench for running the experiments
            logger.info('Installing Filebench')
            configurator = filebench_configurator()
            configurator.install_filebench(kube_workers)

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
