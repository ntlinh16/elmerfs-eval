# Benchmarking the elmerfs

This project uses [cloudal](https://github.com/ntlinh16/cloudal) to automatically perform a full factorial experiment workflow which measures the convergence time, the read/write performance and contentions of [elmerfs](https://github.com/scality/elmerfs), which is a file system using an [AntidoteDB](https://www.antidoteDB.eu/) cluster as a backend.

If you do not install and configure all dependencies to use cloudal, please follow the [instruction](https://github.com/ntlinh16/cloudal)
## I. Experiment workflow

The workflow of this experiment follows [the general experiment flowchart of cloudal](https://github.com/ntlinh16/cloudal/blob/master/docs/technical_detail.md#an-experiment-workflow-with-cloudal).

First, the `create_combs_queue()` function generates combinations of the following parameters: `iteration`, `latency` and `benchmarks`.

After that, the `setup_env()` function performs (1) the provisioning process to make a reservation for the required infrastructure; and (2) the configuration of the provisioned hosts by deploying a Kubernetes cluster and AntidoteDB clusters; and then elmerfs is installed on hosts which run AntidoteDB instances.

Then, the `run_exp_workflow()` takes one combination as the input and performs the following steps:
1. Clean the experiment environment on related nodes
2. Set the latency for a specific run
3. Perform a given [benchmark workflow](https://github.com/ntlinh16/elmerfs-eval#iii-benchmark-workflows)
4. Reset the latency to normal
5. Retrieve the results.

## II. How to run the experiment
### 1. Clone the repository:
Clone the project from the git repo:
```
git clone https://github.com/ntlinh16/elmerfs-eval.git
```
### 2. Prepare the deployment and configuration files

Before running the experiments, you have to prepare the following files:

#### 2.1. Experiment deployment files for Kubernetes

In this experiment, I use Kubernetes deployment (YAML) files to deploy and manage the AntidoteDB cluster as well as the monitoring system (Grafana and Prometheus). Therefore, you need to provide these deployment files. I already provided the template files which work well with this experiment in [exp_config_files](https://github.com/ntlinh16/elmerfs-eval/tree/main/exp_config_files) folder. If you do not require any special configurations, you do not have to modify these files.

#### 2.2. Experiment environment config file

This file contains the infrastructure requirements as well as the experiment parameters of your experiments. It depends on where you want to run the experiments, Grid5000 or OVH platform, you have to provide different information. Follow the next Section to modify this configuration information.

### 3. Run the experiment

To run this experiment on specific cloudal system, please find the detail instruction in the following links:

[Running on Grid5000](https://github.com/ntlinh16/elmerfs-eval/tree/main/grid5k#readme)

[Running on OVH](https://github.com/ntlinh16/elmerfs-eval/blob/tree/main/ovh#readme)

### 4. Parse the results
After finishing the experiment, all the data will be downloaded to your given result directory.
To parse the results in to csv file for analyzing later, run the command:

```bash
python parse_result.py -i <path/to/your/result/directory> -o <path/to/the/output/result/file> OPTION
```
for example:
```bash
python parse_result.py -i results -o results/convergence.csv --convergence
```

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

## Docker images used in this project
I use Docker images to pre-build the environment for AntidoteDB services. All images are on Docker repository:

* antidotedb/antidote:latest
* peterzel/antidote-connect


