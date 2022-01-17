## 1. Instantiate API Keys for your script
Visit https://api.ovh.com/createToken to create your credentials

## 2. Prepare the configuration files

You need to clarify three following information in the `exp_setting_elmerfs_eval_ovh.yaml` file.

* Infrastructure requirements: includes your authorization information, the instance type and the name of the OS you want to deploy on reserved nodes; etc. 


* Parameters: is a list of experiment parameters that represent different aspects of the system that you want to examine. Each parameter contains a list of possible values of that aspect.
    * `iteration: [1..2]`: the experiment will be repeat 5 times for a statistically significant result.
    * `latency: [20, 1000]`: while performing benchmark, the latency between AntidoteDB clusters is change from 20ms to 1 second
    * `latency_interval: logarithmic scale`: the increasing interval of latency will be calculated by logarithmic scale
    * `benchmarks: [convergence, performances, contentions]`: three benchmarks will be used to test elmerfs

* Experiment environment settings: the path to the results directory; the path to Kubernetes deployment files for AntidoteDB and monitoring system; the elmerfs version information that you want to deploy; the topology of an AntidoteDB cluster; etc.

## 3. Run the experiment

```bash
cd elmerfs-eval/ovh
python elmerfs_eval_ovh.py --system_config_file exp_setting_elmerfs_eval_ovh.yaml -k --monitoring &> result/test.log
```

You can watch the log by:
```bash
tail -f elmerfs-eval/result/test.log 
```

* `--monitoring`: if you use this parameter, the script will deploy [Grafana](https://grafana.com/) and [Prometheus](https://prometheus.io) as an AntidoteDB monitoring system. If you use this option, please make sure that you provide the corresponding Kubernetes deployment files. You can connect to the url provided in the log to access the monitoring UI (i.e, `http://<kube_master_ip>:3000`). The default account credential is `admin/admin`. When login successfully, you can search for `Antidote` to access the pre-defined AntidoteDB dashboard.

<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/elmerfs-eval/main/images/grafana_example_screenshot.png" width="650"/>
    <br>
<p>