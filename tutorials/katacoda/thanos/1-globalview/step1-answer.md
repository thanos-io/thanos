## Answer

**How many series (metrics) we collect overall on all Prometheus instances we have?**

How to get this information? As you probably guess it's not straightforward. The current step would be:

* Query <a href="https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=sum(prometheus_tsdb_head_series)&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">Prometheus-0 EU1</a> for `prometheus_tsdb_head_series`
* Query <a href="https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=sum(prometheus_tsdb_head_series)&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">Prometheus-0 US1</a> or <a href="https://[[HOST_SUBDOMAIN]]-9092-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=sum(prometheus_tsdb_head_series)&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">Prometheus-1 US1</a> for `prometheus_tsdb_head_series`
Both holds the same data (number of series for each replica) so we just need to choose available one.
* Sum both results manually.

As you can see this is not very convenient for both human as well as automation on top of metrics (e.g Alerting).

The feature we are missing here is called **Global View** and it might be necessary once you scale out Prometheus to multiple instances.

Great! We have now running 3 Prometheus instances.

In the next steps we will learn how we can install Thanos on top of our initial Prometheus setup to solve problems shown in the challenge.
