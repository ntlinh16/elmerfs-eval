# Benchmarking the elmerfs on Grid5000 system

This project uses [cloudal](https://github.com/ntlinh16/cloudal) to automatically perform a full factorial experiment workflow which measures the convergence time, the read/write performance and contentions of [elmerfs](https://github.com/scality/elmerfs), which is a file system using an [AntidoteDB](https://www.antidoteDB.eu/) cluster as a backend.

## I. Run the experiment

### 1. Prepare the system config file

There are two types of config files to perform this experiment.

#### AntidoteDB Kubernetes deployment files 

I use Kubernetes deployment files to deploy an AntidoteDB cluster for this experiment. These files are provided in folder [antidotedb_yaml](https://github.com/ntlinh16/cloudal/tree/master/examples/experiment/elmerfs/antidotedb_yaml) and they work well for this experiment scenario. Check and modify these template files if you need any special configurations for AntidoteDB.


#### Experiment environment config file

You need to clarify three following information in the `exp_setting_elmerfs_eval_g5k.yaml` file.

* Infrastructure requirements: includes the number of clusters, name of cluster and the number of nodes for each cluster you want to provision on Grid5k system; which OS you want to deploy on reserved nodes; when and how long you want to provision nodes; etc.


* Parameters: is a list of experiment parameters that represent different aspects of the system that you want to examine. Each parameter contains a list of possible values of that aspect.
    * `iteration: [1..2]`: the experiment will be repeat 5 times for a statistically significant result.
    * `latency: [20, 1000]`: while performing benchmark, the latency between AntidoteDB clusters is change from 20ms to 1 second
    * `latency_interval: logarithmic scale`: the increasing interval of latency will be calculated by logarithmic scale
    * `benchmarks: [convergence, performances, contentions]`: three benchmarks will be used to test elmerfs

* Experiment environment settings: the path to Kubernetes deployment files for AntidoteDB and monitoring system; the elmerfs version information that you want to deploy; the topology of an AntidoteDB cluster; etc.
### 2. Run the experiment
If you are running this experiment on your local machine, remember to run the VPN to [connect to Grid5000 system from outside](https://github.com/ntlinh16/cloudal/blob/master/docs/g5k_k8s_setting.md).

Then, run the following command:

```bash
cd cloudal/examples/experiment/elmerfs/
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k --monitoring &> result/test.log
```
You can watch the log by:
```bash
tail -f cloudal/examples/experiment/antidotedb/result/test.log 
```
Arguments:

* `-k`: after finishing all the runs of the experiment, al provisioned nodes on Gris5000 will be kept alive so that you can connect to them, or if the experiment is interrupted in the middle, you can use these provisioned nodes to continue the experiments. This mechanism saves time since you don't have to reserve and deploy nodes again. If you do not use `-k`, when the script is finished or interrupted, all your reserved nodes will be deleted.
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
cd cloudal/examples/experiment/elmerfs/
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k --monitoring &> result/test.log
```
This command performs `setup_env()` to provision and configure the required experiment environment; and then run the experiment workflow with the remaining combinations.

b. If your reserved hosts are still alive, you can give it to the script (to ignore the provisioning process):

```bash
cd cloudal/examples/experiment/elmerfs/
python elmerfs_eval_g5k.py --system_config_file exp_setting_elmerfs_eval_g5k.yaml -k -j <site1:oar_job_id1,site2:oar_job_id2,...> --no-deploy-os --kube-master <host_name_of_kubernetes_master> --monitoring &> result/test.log
```
This command re-deploy the AntidoteDB clusters, elmerfs instances and monitoring system, then continues to run the experiment workflow for the remaining combinations on the pre-deployed infrastructure which are the provisioned nodes and the deployed Kubernetes cluster.

## II. Experiment workflow

The workflow of this experiment follows [the general experiment flowchart of cloudal](https://github.com/ntlinh16/cloudal/blob/master/docs/technical_detail.md#an-experiment-workflow-with-cloudal).

The `create_combs_queue()` function generates combinations of the following parameters: `iteration`, `latency` and `benchmarks`.

The `setup_env()` function performs (1) the provisioning process to make a reservation for the required infrastructure; and (2) the configuration of the provisioned hosts by deploying a Kubernetes cluster and AntidoteDB clusters; and then elmerfs is installed on hosts which run AntidoteDB instances.

The `run_exp_workflow()` takes one combination as the input and performs the following steps:
1. Clean the experiment environment on related nodes
2. Set the latency for a specific run
3. Perform a given [benchmark workflow](https://github.com/ntlinh16/elmerfs-eval#benchmarks-workflow)
4. Reset the latency to normal
5. Retrieve the results.
## III. Benchmark workflows
In this work, we performs different benchmarks on elmerfs to study the characteristics of this distributed file system. The detail of each benchmark workflow is provided in the following section.
### 1. Convergence time
The purpose of this experiment is to identify (1) whether the data on elmerfs can converge into a same state across hosts on the system; and (2) the time duration for which the data is converged. The converged state is identified as the checksum between the data among different hosts.

The overall workflow to measure the convergence time described as follow:

1. Chose randomly the source host in a cluster, and mark all other hosts as destination hosts.
2. Start the checksum process script to calculate the checksum of data on those destination hosts and verify them against the pre-calculated checksum of the data in the source host.
3. Copy a file (or create a folder tree) to the elmerfs mount point on the source host. As the data on the source host is propagated on destination hosts with AntidoteDB as the backend, the checksum process script in step 2 continuously comparing the checksum of its current data to check for the convergence state. 
4. Waiting for the checksum process script on all destination hosts to finish.

There are three scenarios to measure the convergence time in this experiment.
#### Convergence time of sync a single file
In this scenario, first, we start a script on all destination hosts to calculate the checksum of a file in elmerfs mount point; then we start a command to copy a file into the elmerfs mount point on the source host; next we wait for all the checksum process on all destination hosts to finish.

The convergence time is measured as the duration between the time we start the copy command to copy a file to elmerfs mount point on a source host and the time when the checksum process on a destination host completed. 

For each run, we change the latency between the antidoteDB clusters. The results show the convergence time in the antidoteDB cluster. This helps us evaluate how does the latency impact the convergence time in the distributed file system - elmerfs.

#### Convergence time of sync a file with different file size
In this scenario, the workflow of each run is similar to the scenario of `convergence time of sync a single file`. However, instead of change the latency, we change the size of the file, this allow us to have an overview whether the convergence time is impacted by the size of the synchronized file?

#### Convergence time of sync a folder

In this scenario, the workflow of each run is similar to the scenario of `convergence time of sync a single file`. However, instead of copy a file to the elmerfs mount point, we create a folder tree. We want to evaluate how does latency impact to folder creating operation.

