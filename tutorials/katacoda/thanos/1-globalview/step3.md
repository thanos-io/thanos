# Step 3 - Adding Thanos Querier

Thanks to the previous step we have three running Prometheus instances with a sidecar each. In this step we will install
Thanos Querier which will use sidecars and allow querying all metrics from the single place as presented below:

![with querier](https://docs.google.com/drawings/d/e/2PACX-1vSB9gN82px0lxk9vN6wNw3eXr8Z0EVROW3xubsq7tgjbx_nXsoZ02ElzvxeDevyjGPWv-f9Gie0NeNz/pub?w=926&h=539)

But before that, let's take a closer look at what the Querier component does:

## Querier

The Querier component (also called "Query") is essentially a vanilla PromQL Prometheus engine that fetches the data from any service
that implements Thanos [StoreAPI](https://thanos.io/tip/thanos/integrations.md/#storeapi). This means that Querier exposes the Prometheus HTTP v1 API to query the data in a common PromQL language.
This allows compatibility with Grafana or other consumers of Prometheus' API.

Additionally, Querier is capable of deduplicating StoreAPIs that are in the same HA group. We will see how it
looks in practice later on.

You can read more about Thanos Querier [here](https://thanos.io/tip/components/query.md/)

## Deploying Thanos Querier

Let' now start the Query component. As you remember [Thanos sidecar](https://thanos.io/tip/components/query.md/) exposes `StoreAPI`
so we will make sure we point the Querier to the gRPC endpoints of all our three sidecars:

Click below snippet to start the Querier.

```
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.19.0 \
    query \
    --http-address 0.0.0.0:29090 \
    --query.replica-label replica \
    --store 127.0.0.1:19190 \
    --store 127.0.0.1:19191 \
    --store 127.0.0.1:19192 && echo "Started Thanos Querier"
```{{execute}}

## Setup verification

Thanos Querier exposes very similar UI to the Prometheus, but on top of many `StoreAPIs you wish to connect to.

To check if the Querier works as intended let's look on [Querier UI `Store` page](https://[[HOST_SUBDOMAIN]]-29090-[[KATACODA_HOST]].environments.katacoda.com/stores).

This should list all our three sidecars, including their external labels.

## Global view - Not challenging anymore?

Now, let's get back to our challenge from step 1, so finding the answer to  **How many series (metrics) we collect overall on all Prometheus instances we have?**

With the querier this is now super simple.

It's just enough to query Querier for <a href="https://[[HOST_SUBDOMAIN]]-29090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=sum(prometheus_tsdb_head_series)&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">`sum(prometheus_tsdb_head_series)`</a>

You should see the single value representing the number of series scraped in both clusters in the current mode.

If we query `prometheus_tsdb_head_series` we will see that we have complete info about all three Prometheus instances:

```
prometheus_tsdb_head_series{cluster="eu1",instance="127.0.0.1:9090",job="prometheus"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9091",job="prometheus"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9092",job="prometheus"}
```

## Handling of Highly Available Prometheus

Now, as you remember we configured Prometheus 0 US1 and Prometheus 1 US1 to scrape the same things. We also connect Querier
to both, so how Querier knows what is an HA group?

Try to query the same query as before: <a href="https://[[HOST_SUBDOMAIN]]-29090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=sum(prometheus_tsdb_head_series)&g0.tab=1&g1.range_input=5m&g1.expr=prometheus_tsdb_head_series&g1.tab=0">`prometheus_tsdb_head_series`</a>

Now turn off deduplication (`deduplication` button on Querier UI) and hit `Execute` again. Now you should see 5 results:

```
prometheus_tsdb_head_series{cluster="eu1",instance="127.0.0.1:9090",job="prometheus",replica="0"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9091",job="prometheus",replica="0"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9091",job="prometheus",replica="1"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9092",job="prometheus",replica="0"}
prometheus_tsdb_head_series{cluster="us1",instance="127.0.0.1:9092",job="prometheus",replica="1"}
```

So how Thanos Querier knows how to deduplicate correctly?

If we would look again into Querier configuration we can see that we also set `query.replica-label` flag.
This is exactly the label Querier will try to deduplicate by for HA groups. This means that any metric with exactly
the same labels *except replica label* will be assumed as the metric from the same HA group, and deduplicated accordingly.

If we would open `prometheus1_us1.yml` config file in the editor or if you go to Prometheus 1 US1 [/config](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/config).
you should see our external labels in `external_labels` YAML option:

```yaml
  external_labels:
    cluster: us1
    replica: 1
```

Now if we compare to `prometheus0_us1.yaml`:

```yaml
  external_labels:
    cluster: us1
    replica: 0
```

We can see that since those two replicas scrape the same targets, any metric will be produced twice.
Once by `replica=1, cluster=us1` Prometheus and once by `replica=0, cluster=us1` Prometheus. If we configure Querier to
deduplicate by `replica` we can transparently handle this High Available pair of Prometheus instances to the user.

## Production deployment

Normally Querier runs in some central global location (e.g next to Grafana) with remote access to all Prometheus-es (e.g via ingress, proxies vpn or peering)

You can also stack (federate) Queriers on top of other Queries, as Query expose `StoreAPI` as well!

More information about those advanced topics can be found in the next courses that will be added soon.

## Next

Awesome! Feel free to play around with the following setup:

![with querier](https://docs.google.com/drawings/d/e/2PACX-1vSB9gN82px0lxk9vN6wNw3eXr8Z0EVROW3xubsq7tgjbx_nXsoZ02ElzvxeDevyjGPWv-f9Gie0NeNz/pub?w=926&h=539)

Once done hit `Continue` for summary.
