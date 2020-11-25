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
	namespace = "tdjobs"
)

type tdJobCollector struct {
	ApiKey string
}

var (
	runningJobCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "running_counter",
		Help: "job_counter",
	})
)

func (c tdJobCollector) Describe(ch chan <- *prometheus.Desc) {
	ch <- runningJobCount.Desc()
}

func (c tdJobCollector) Collect(ch chan <- prometheus.Metric) {
	count, err := c.getJobCount()
	if err != nil {
		log.Fatalf("error happened: %s\n", err)
	}
	ch <- prometheus.MustNewConstMetric(
		runningJobCount.Desc(),
		prometheus.CounterValue,
		float64(count),
		)
}

func (c tdJobCollector) getJobCount() (int, error) {
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: c.ApiKey,
	})
	if err != nil {
		log.Fatalf("Something went wrong when you create TD Clinet instance: %s", err)
	}
	var jobOptions = td_client.ListJobsOptions{}
	jobOptions.WithStatus("running")
	jl, err := client.ListJobsWithOptions(&jobOptions)
	if err != nil {
		panic(err)
	}
	list := jl.Count
	return list, nil
}


func main() {
	var (
		addr = flag.String("listen-address", "127.0.0.1:5000", "The address to listen on for HTTP requests.")
		apikey = flag.String("td-apikey","","Treasure Data API Key")
		)

	flag.Parse()

	c := tdJobCollector{
		ApiKey: *apikey,
	}
	prometheus.MustRegister(c)

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
