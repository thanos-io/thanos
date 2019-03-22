# Thanosbench
A series of benchmarking/stress test tools for [thanos](https://github.com/improbable-eng/thanos).

## Installation
1. Fetch thanosbench `go get github.com/improbable-eng/thanosbench`
1. Install thanosbench with `make`
1. Create cloud resources for the load test k8s cluster.
   * `cd terraform`
   * `terraform init`
   * `terraform plan`
   * `terraform apply`
1. Run a benchmark `thanosbench --help`

## Results
Thanos performance will depend heavily on the specific use case (number of scrapers, scrape interval, number of timeseries, etc..), so we recommend forking & adjusting this tool to suit your situation. However, we have included below some general results discovered from running these tools.

### Thanos vs prometheus
We started a prometheus instance with thanos sidecar, scraping 50 targets every second. We used this to collect 15 minutes of metrics (4.5 million total samples). We ran the following queries over both `thanos query` and regular prometheus endpoints.
1. Rate over 4.5 million metrics: `rate({__name__=~"ts_.*"}[1m])`
   * Thanos query
      * 5.79s average query time (min: 3.06s, max: 6.16s)
      * *0.172 queries per second*
   * Prometheus
      * 3.13 average query time (min: 3.06s, max: 3.91s)
      * *0.320 queries per second*
1. Sum over 4.5 million metrics `sum({__name__=~”ts_.*”}) by (instance)`
   * Thanos query
      * 3.77s average query time (min: 3.62s, max: 3.98s)
      * *0.265 queries per second*
   * Prometheus
      * 1.23 average query time (min: 1.14s, max: 1.47s)
      * *0.812 queries per second*
1. Fetch 45 thousand metrics from a single timeseries `ts_00`
   * Thanos query
      * 0.055s average query time (min: 0.043s, max: 0.135s)
      * *18.2 queries per second*
   * Prometheus
      * 0.022 average query time (min: 0.018s, max: 0.073s)
      * *0.812 queries per second*

This shows an added latency of *85-150%* when using thanos query. This is more or less expected, as network operations will have to be done twice. We took a profile of thanos query while under load, finding that about a third of the time is being spent evaluating the promql queries. We are looking into including newer versions of the prometheus libraries into thanos that include optimisations to this component.

Although we have not tested federated prometheus in the same controlled environment, in theory it should incur a similar overhead, as we will still be performing two network hops.

### Store performance
To test the store component, we generated 1 year of simulated metrics (100 timeseries taking random values every 15s, a total of 210 million samples). We were able to run heavy queries to touch all 210 million of these samples, e.g. a sum over 100 timeseries takes about 34.6 seconds. Smaller queries, for example fetching 1 year of samples from a single timeseries, were able to run in about 500 milliseconds.

When enabling downsampling over these timeseries, we were able to reduce query times by over 90%.

### Ingestion
To try to find the limits of a single thanos-query service, we spun up a number prometheus instances, each scraping 10 metric-producing endpoints every second. We attached a thanos-query endpoint in front of these scrapers, and ran queries that would touch fetch a most recent metric from each of them. Each metric producing endpoint would serve 100 metrics, taking random values, and the query would fetch the most recent value from each of these metrics:
* 10 scrapers, 100 total metric producing endpoints, ingesting 10k samples/second
   * ~0.05 second query response
* 100 scrapers, 1000 total metric producing endpoints, ingesting 100k samples/second
   * ~0.20 second query response
* 200 scrapers, 2000 total metric producing endpoints, ingesting 200k samples/second
  * ~1.8 second query response time

