# Benchmarking the elmerfs on Grid5000 system

This project uses [cloudal](https://github.com/ntlinh16/cloudal) to perform a  full factorial experiment workflow automatically. This experiment measures the convergence time, the read/write performance and contentions (which described in detail later) of [elmerfs](https://github.com/scality/elmerfs), which is a file system using an [AntidoteDB](https://www.antidoteDB.eu/) cluster as a backend.


## Introduction

The workflow of this experiment follows [the general experiment flowchart of cloudal](https://github.com/ntlinh16/cloudal/blob/master/docs/technical_detail.md#an-experiment-workflow-with-cloudal).

The `create_combs_queue()` function generates combinations of the following parameters: iteration, latency and benchmarks.

* `iteration: [1..2]`: the experiment will be repeat 5 times for a statistically significant result.
* `latency: [20, 1000]`: while performing benchmark, the latency between AntidoteDB clusters is change from 20ms to 1 second
* `latency_interval: logarithmic scale`: the increasing interval of latency will be calculated by logarithmic scale 
* `benchmarks: [convergence, performances, Contentions]`: three benchmarks will be used to test elmerfs

The `setup_env()` function the provisioning and the configuring process
* `provisioning`: makes a reservation for the required infrastructure;
* `config_host()`: configuring these hosts by deploying a Kubernetes cluster to manage a AntidoteDB cluster; and then elmerfs is install on hosts which run AntidoteDB instances.

The `run_exp_workflow()` performs the following steps:
1. Clean the experiment environment on related nodes
2. Set the latency for a specific run
3. Perform a given benchmark
4. Reset the latency
5. Retrieve the results.

## How to run the experiment

### 1. Prepare the system config file

There are two types of config files to perform this experiment.

#### Setup environment config file
The system config file `exp_setting_elmerfs_eval_g5k.yaml` provides three following information:

* Infrastructure requirements: includes the number of clusters, name of cluster and the number of nodes for each cluster you want to provision on Grid5k system; which OS you want to deploy on reserved nodes; when and how long you want to provision nodes; etc.


* Parameters: is a list of experiment parameters that represent different aspects of the system that you want to examine. Each parameter contains a list of possible values of that aspect. For example, I want to achieve statistically significant results so that each experiment will be repeated 5 times  with parameter `iteration: [1..5]`.

* Experiment environment settings: the path to Kubernetes deployment files for Antidote; the elmerfs version information that you want to deploy; the topology of an AntidoteDB cluster; etc.

You need to clarify all these information in `exp_setting_elmerfs_g5k.yaml` file

#### Experiment config files 

I use Kubernetes deployment files to deploy an AntidoteDB cluster for this experiment. These files are provided in folder [antidotedb_yaml](https://github.com/ntlinh16/cloudal/tree/master/examples/experiment/elmerfs/antidotedb_yaml) and they work well for this experiment. Check and modify these template files if you need any special configurations for AntidoteDB.

### 2. Run the experiment
If you are running this experiment on your local machine, remember to run the VPN to [connect to Grid5000 system from outside](https://github.com/ntlinh16/cloudal/blob/master/docs/g5k_k8s_setting.md).

Then, run the following command:

```
cd cloudal/examples/experiment/elmerfs/
python elmerfs_g5k.py --system_config_file exp_setting_elmerfs_g5k.yaml -k
```

### 3. Re-run the experiment
If the script is interrupted by unexpected reasons. You can re-run the experiment and it will continue with the list of combinations left in the queue. You have to provide the same result directory of the previous one. There are two possible cases:

1. If your reserved hosts are dead, you just run the same above command:
```
cd cloudal/examples/experiment/elmerfs/
python elmerfs_g5k.py --system_config_file exp_setting_elmerfs.yaml -k
```

2. If your reserved hosts are still alive, you can give it to the script (to ignore the provisioning process):

```
cd cloudal/examples/experiment/elmerfs/
python elmerfs_g5k.py --system_config_file exp_setting_elmerfs_g5k.yaml -k -j <site1:oar_job_id1,site2:oar_job_id2,...> --no-deploy-os --kube-master <the host name of the kubernetes master>
```
