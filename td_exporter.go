package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	td_client "github.com/treasure-data/td-client-go"
)

const (
	namespace = "tdjobs_status"
)

type tdJobCollector struct {
	ApiKey string
	Region td_client.EndpointRouter
}

var (
	runningPrestoJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_presto_counter",
		Help:      "running_presto_jobs",
	})

	runningHiveJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_hive_counter",
		Help:      "running_hive_jobs",
	})

	runningBulkloadJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_bulkload_counter",
		Help:      "running_bulkload_jobs",
	})

	runnginBulkImportJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_bulkimport_counter",
		Help:      "running_bulkimport_jobs",
	})

	runnginResultExportJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_resultexport_counter",
		Help:      "running_resultexport_jobs",
	})

	queuedPrestoJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_presto_counter",
		Help:      "queued_presto_counter",
	})

	queuedHiveJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_hive_counter",
		Help:      "queued_hive_counter",
	})

	queuedBulkloadJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_bulkload_counter",
		Help:      "queued_bulkload_counter",
	})

	queuedBulkImportJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_bulkimport_counter",
		Help:      "queued_bulkimport_counter",
	})

	queuedResultExportJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_resultexport_counter",
		Help:      "queued_esultexport_jobs",
	})
)

func (c tdJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- runningPrestoJobCount.Desc()
	ch <- runningHiveJobCount.Desc()
	ch <- runningBulkloadJobCount.Desc()
	ch <- runnginBulkImportJobCount.Desc()
	ch <- runnginResultExportJobCount.Desc()
	ch <- queuedPrestoJobCount.Desc()
	ch <- queuedHiveJobCount.Desc()
	ch <- queuedBulkloadJobCount.Desc()
	ch <- queuedBulkImportJobCount.Desc()
	ch <- queuedResultExportJobCount.Desc()
}

func (c tdJobCollector) Collect(ch chan<- prometheus.Metric) {
	countPerType, err := c.getRunningJobCount()
	if err != nil {
		log.Fatalf("error happened: %s\n", err)
	}

	log.Printf("the running joblist is %+v", countPerType)
	ch <- prometheus.MustNewConstMetric(
		runningPrestoJobCount.Desc(),
		prometheus.CounterValue,
		float64(countPerType["presto"]),
	)

	ch <- prometheus.MustNewConstMetric(
		runningHiveJobCount.Desc(),
		prometheus.CounterValue,
		float64(countPerType["hive"]),
	)

	ch <- prometheus.MustNewConstMetric(
		runningBulkloadJobCount.Desc(),
		prometheus.CounterValue,
		float64(countPerType["bulkload"]),
	)

	ch <- prometheus.MustNewConstMetric(
		runnginBulkImportJobCount.Desc(),
		prometheus.CounterValue,
		float64(countPerType["bulk_import_perform"]),
	)

	ch <- prometheus.MustNewConstMetric(
		runnginResultExportJobCount.Desc(),
		prometheus.CounterValue,
		float64(countPerType["result_export"]),
	)

	qcount, err := c.getQueuedJobCount()
	log.Printf("the queued joblist is %+v", qcount)
	if err != nil {
		log.Fatalf("error happened: %s\n", err)
	}

	ch <- prometheus.MustNewConstMetric(
		queuedPrestoJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount["presto"]),
	)

	ch <- prometheus.MustNewConstMetric(
		queuedHiveJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount["hive"]),
	)

	ch <- prometheus.MustNewConstMetric(
		queuedBulkloadJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount["bulkload"]),
	)
	ch <- prometheus.MustNewConstMetric(
		queuedBulkImportJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount["bulk_import_perform"]),
	)

	ch <- prometheus.MustNewConstMetric(
		queuedResultExportJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount["result_export"]),
	)
}

func (c *tdJobCollector) getRunningJobCount() (map[string]int, error) {
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: c.ApiKey,
		Router: c.Region,
	})

	if err != nil {
		log.Fatalf("Something went wrong when you create TD Clinet instance: %s", err)
	}
	var jobOptions = td_client.ListJobsOptions{}
	jobOptions.WithStatus("running")
	jl, err := client.ListJobsWithOptions(&jobOptions)
	if err != nil {
		log.Fatalf("Something went wrong during get running jobs: %s", err)
	}

	runningJobList := map[string]int{}
	for i := range jl.ListJobsResultElements {
		if _, ok := runningJobList[jl.ListJobsResultElements[i].Type]; !ok {
			runningJobList[jl.ListJobsResultElements[i].Type] = 1
		} else {
			runningJobList[jl.ListJobsResultElements[i].Type]++
		}
	}
	
	return runningJobList, nil
}

func (c *tdJobCollector) getQueuedJobCount() (map[string]int, error) {
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: c.ApiKey,
		Router: c.Region,
	})
	if err != nil {
		log.Fatalf("Something went wrong when you create TD Clinet instance: %s", err)
	}
	var jobOptions = td_client.ListJobsOptions{}
	jobOptions.WithStatus("queued")
	jl, err := client.ListJobsWithOptions(&jobOptions)
	if err != nil {
		log.Fatalf("failed to list queued jobs: %s", err)
	}
	queuedJobList := map[string]int{}

	for i := range jl.ListJobsResultElements {
		if _, ok := queuedJobList[jl.ListJobsResultElements[i].Type]; !ok {
			queuedJobList[jl.ListJobsResultElements[i].Type] = 1
		} else {
			queuedJobList[jl.ListJobsResultElements[i].Type]++
		}
	}
	
	return queuedJobList, nil
}

type Endpoint struct {
	endpoint string
}

func (e Endpoint) Route(s string) string {
	return e.endpoint
}

func main() {
	var (
		addr   = flag.String("listen-address", "127.0.0.1:5000", "The address to listen on for HTTP requests.")
		apikey = flag.String("td-apikey", "", "Treasure Data API Key")
		region = flag.String("endpoint", td_client.DEFAULT_ENDPOINT, "Treasure Data Region")
	)

	flag.Parse()
	endpoint := Endpoint{endpoint: *region}
	if *apikey == "" {
		log.Fatal("You must set apikey")
	}
	c := tdJobCollector{
		ApiKey: *apikey,
		Region: endpoint,
	}
	prometheus.MustRegister(c)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
