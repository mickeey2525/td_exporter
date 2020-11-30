package main

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	td_client "github.com/treasure-data/td-client-go"
	"log"
	"net/http"
)

const (
	namespace = "tdjobs_status"
)

type tdJobCollector struct {
	ApiKey string
	Region td_client.EndpointRouter
}

var (
	runningJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "running_counter",
		Help:      "running_jobs",
	})

	queuedJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queued_counter",
		Help:      "job_counter",
	})
)

func (c tdJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- runningJobCount.Desc()
	ch <- queuedJobCount.Desc()
}

func (c tdJobCollector) Collect(ch chan<- prometheus.Metric) {
	count, err := c.getRunningJobCount()
	if err != nil {
		log.Fatalf("error happened: %s\n", err)
	}
	ch <- prometheus.MustNewConstMetric(
		runningJobCount.Desc(),
		prometheus.CounterValue,
		float64(count),
	)

	qcount, err := c.getQueuedJobCount()
	if err != nil {
		log.Fatalf("error happened: %s\n", err)
	}
	ch <- prometheus.MustNewConstMetric(
		queuedJobCount.Desc(),
		prometheus.CounterValue,
		float64(qcount),
	)
}

func (c *tdJobCollector) getRunningJobCount() (int, error) {
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
		log.Fatalf("Something went wrong during get queued jobs: %s", err)
	}
	list := jl.Count
	return list, nil
}

func (c *tdJobCollector) getQueuedJobCount() (int, error) {

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
		log.Fatalf("Something went wrong during get queued jobs: %s", err)
	}
	list := jl.Count
	return list, nil
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
