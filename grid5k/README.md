## 1. Prepare the configuration files

You need to clarify three following information in the `exp_setting_elmerfs_eval_g5k.yaml` file.

* Infrastructure requirements: includes the number of clusters, name of cluster and the number of nodes for each cluster you want to provision on Grid5k system; which OS you want to deploy on reserved nodes; when and how long you want to provision nodes; etc.


* Parameters: is a list of experiment parameters that represent different aspects of the system that you want to examine. Each parameter contains a list of possible values of that aspect.
    * `iteration: [1..2]`: the experiment will be repeat 5 times for a statistically significant result.
    * `benchmarks: [mailserver, videoserver]`: three benchmarks will be used to test elmerfs
    * `duration: 600`: the time you want to run the workload
    * `n_client: [1,2,3]`: the number of benckmark processes you want to generate (each benckmark processes will run on one node)
    *  `n_nodes_per_dc: [3,6,9]`: the number of AntidoteDB node in a cluster
    

* Experiment environment settings: the path to the results directory; the path to Kubernetes deployment files for AntidoteDB and monitoring system; the elmerfs version information that you want to deploy; the topology of an AntidoteDB cluster; etc.

### 2. Run the experiment
If you are running this experiment on your local machine, remember to run the VPN to [connect to Grid5000 system from outside](https://github.com/ntlinh16/cloudal/blob/master/docs/g5k_k8s_setting.md).

Then, run the following command:

```bash
cd elmerfs-eval/grid5k/
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k --monitoring &> result/test.log
```
You can watch the log by:
```bash
tail -f elmerfs-eval/grid5k/result/test.log 
```
Arguments:

* `-k`: after finishing all the runs of the experiment, all provisioned nodes on Gris5000 will be kept alive so that you can connect to them, or if the experiment is interrupted in the middle, you can use these provisioned nodes to continue the experiments. This mechanism saves time since you don't have to reserve and deploy nodes again. If you do not use `-k`, when the script is finished or interrupted, all your reserved nodes will be deleted.
* `--monitoring`: the script will deploy [Grafana](https://grafana.com/) and [Prometheus](https://prometheus.io/) as an AntidoteDB monitoring system. If you use this option, please make sure that you provide the corresponding Kubernetes deployment files. You can connect to the url provided in the log to access the monitoring UI (i.e., `http://<kube_master_ip>:3000`). The default account credential is `admin/admin`. When login successfully, you can search for `Antidote` to access the pre-defined AntidoteDB dashboard.
<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/elmerfs-eval/main/images/grafana_example_screenshot.png" width="650"/>
    <br>
<p>
         
### 3. Re-run the experiment
If the script is interrupted by unexpected reasons. You can re-run the experiment and it will continue with the list of combinations left in the queue. You have to provide the same result directory of the previous one. There are two possible cases:

a. If your reserved hosts are dead, you just run the same above command:
```bash
cd elmerfs-eval/grid5k
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k --monitoring &> result/test.log
```
This command performs `setup_env()` to provision and configure the required experiment environment; and then run the experiment workflow with the remaining combinations.

b. If your reserved hosts are still alive, you can give it to the script (to ignore the provisioning process):

```bash
cd elmerfs-eval/grid5k
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k -j <site1:oar_job_id1,site2:oar_job_id2,...> --no-deploy-os --kube-master <host_name_of_kubernetes_master> --monitoring &> result/test.log
```
This command re-deploy the AntidoteDB clusters, elmerfs instances and monitoring system, then continues to run the experiment workflow for the remaining combinations on the pre-deployed infrastructure which are the provisioned nodes and the deployed Kubernetes cluster.