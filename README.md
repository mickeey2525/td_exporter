td_exporter
===

td_exporter is an unofficial Prometheus Exporter for Treasure Data  
With this exporter, you can check the current running jobs 

### How to use
#### Build td_exporter

1. Download files
    ```
    git clone git@github.com:mickeey2525/td_exporter.git
    cd td_exporter
    ```

2. Build the file
    ```
    go build 
    ```

3. Run the command
    ```
    chmod +x td_exporer
    ./td_exporter -td-apikey Your_APIKEY -endpoint yourEndpoint
    ```

4. Then you can see metrics page at http://localhost:5000/metrics

### Metrics

Documents about exposed Prometheus Metrics.  

|Name|Exposed Information|
|---|---|
|tdjobs_status_queued_counter|the number of queued jobs. This means some jobs wait for resource release|
|tdjobs_status_running_counter|the number of the current running jobs. All jobs are included|