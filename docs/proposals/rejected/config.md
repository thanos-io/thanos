# Thanos Cluster Configuration

Status: draft | in-review | **rejected** | accepted | complete

Implementation Owner: [@domgreen](https://github.com/domgreen)

## Summary

The proposal of creating a central configuration component within Thanos has been rejected by the community as the requirements are specific to the use case at Improbable and that adding configuration management into Thanos will result in adding more knowledge to the system about what the scrapers are doing and their targets.

Cluster configuration for targets will be implemented in a separate repository and we may look at open sourcing in the future if there are others that have the same needs.

Please see [Issue 387](https://github.com/improbable-eng/thanos/pull/387) and [Prometheus/Prometheus - Issue 4309](https://github.com/prometheus/prometheus/issues/4309) for more information.

## Motivation

Currently, each scraper manages their own configuration via [Prometheus Configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) which contains information about the [scrape_config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#%3Cscrape_config%3E) and targets that the scraper will be collecting metrics from.

As we start to dynamically scale the collection of metrics (new Prometheus instances) or increase the targets for a current tenant we wish to keep the collection of metrics to a consistent node and not re-allocate shards to other scrapers.

The reason for adding this as a component within the thanos project is primarily due to the need of a sidecar to interact with the Prometheus instance collecting metrics. In the scale down scenario when we want to remove all targets from a scraper we would also want to force the collection / upload of the WAL block from the running Prometheus instance before removing the Prometheus instance. During this period of scaling down / draining of the scraper we would also want the sidecar and Prometheus to continue to serve the data from within the Prometheus instance itself till we can be sure that Store node can fetch the data for our users.
ALong with this we are looking to have a sidecar that updates targets via `file_sd_config` this could be a separate sidecar to thanos sidecar but adds another component into the mix. The implementation is also likely to borrow from the thanos codebase to identify which Prometheus instances are in the cluster to assign targets.

This approach is not intended to replace current horizontal scaling strategies in Prometheus but enable new strategies for environments where the number of targets can be highly dynamic and require spinning up and removing scrapers based on the workload. Typically most Prometheus sets would have a static number of targets and therefore able to use `hashmod` within Prometheus. If adding a new scraper infrequent churn from each Prometheus TSDB would be reduced and not impact the system.
We have seen the `hashmod` scaling approach have a problem with hot shards in the past whereby a given Prometheus instance may endup being overloaded by the number or amount of data from its targets and is difficult to redistribute the load onto other Promethus instances in the scrape pool.


## Goals

The primary goal of having a specific `thanos config` component are:

- enable (auto)scaling of scrapers to meet the needs metrics ingestion
  - ensure we can schedule work to new scrapers
  - on scale down ensure we can drain scrapers and upload data before removing
- assign targets to scrapers based on utilisation
- opt-in approach users can and should use existing scaling if it works for their use case

Secondary goals of the proposal would be:

- reduce churn on the underlying Prometheus TSDB
- multi-tenant aware, query minimal scrapers for data
- reduce / mitigate hot sharding issues

## Proposal

As we scale out horizontally we wish to manage configuration such as target assignment to scrapes in a more intelligent way, firstly by keeping targets on the same scrape instance as much as possible as we scale the scrape pool and in the future more intelligent bin packing of targets onto scrapers.

We would look to add a new component to the Thanos system `thanos config` that would be a central point for configuring what each Prometheus scrapes for the entire cluster and advertising via APIs the configuration for each sidecar.

The `thanos config` will have a configuration endpoint that each `thanos sidecar` will call into to get their own scrape_config jobs along with their targets. Once the sidecar has its jobs it will be able to update targets / scrape_config for the Prometheus instance it is running alongside. This update will primarily be based on `file_sd_config` and will be allowed to add or remove targets without changing Prometheus itself.

The config component will keep track of what sidecar's are in the cluster via the existing gossip mechanism and will therefore have a central view of targets to Prometheus instances.
When we scale up our scrape pool and a new scrape instance comes online the `thanos sidecar` join the gossip cluster and therefoe `thanos config` will know that it can start assigning configuration to this new node.
In a scale down scenario we can first remove all targets from a given scraper effectively draining that instance of work and kick off the process of uploading the WAL to storage (not in scope of this design). During the time that the Prometheus instance has no targets we would still want to be able to query the instance for data that has not yet been uploaded.

We believe that a central point for configuration and management is better in this scenario as it gives us more flexibility in the future to add bin packing / consistent hashing. It would also be an ideal place for deciding on "hot shard" issues, the config component would be able to see the utilization of each node and decide based on that where to schedule work. Having a centralised approach would also help with debugging, testing and maintaining the code when issues arise.

```
      ┌─ reload ─┐               ┌─ reload ─┐         (reload via file_sd_config)
      v          │               v          │
┌──────────────────────┐  ┌────────────┬─────────┐
│ Prometheus │ Sidecar │  │ Prometheus │ Sidecar │
└─────────────────┬────┘  └────────────┴────┬────┘
                  │                         │
               GetConfig                GetConfig
                  │                         │
                  v                         v
                ┌─────────────────────────────┐
                │            Config           │
                └──────────────┬──────────────┘
                               │
                           Read files
                               │
                               v
                ┌─────────────────────────────┐
                │            prom.yml         │
                └─────────────────────────────┘
```


## User Experience

### Use Cases

- We would wish to dynamically configure what targets / labels are on each Prometheus instance within the Thanos cluster
- Allocate targets to a Prometheus instance based on data such as CPU utilization of a node.
- Ensure that as we scale the number of Prometheus instances in the scrape pool we do not move scrape targets to other instances.

### Example Use Case

Currently we have the use case whereby a customer can deploy a game into our environment and we may need to dramatically increase the number of targets being scraped.
In doing so we may need to scale the number of instances of Prometheus in our scrape pool; we would want to ensure that it has minimal impact to the existing collection of metrics and ensure we do not over oad any scrapers by the resharding of targets.
This deployment may only last for a number of hours (whilst other may last days, weeks, or months) and then will be removed. Therefore removing the targets and scaling down the number of Prometheus instances in the pool.
It is also worth mentioning in this specific use case that although the Prometheus and Thanos components are deployed within a Kubernetes cluster the targets they are scraping do not have to be within the cluster where the monitoring components live or running within Kubernetes at all.

## Implementation

### Thanos Config

The config component will read one or many Prometheus configuration files and dynamically allocate configuration to sidecars within the cluster. It initially joins a Thanos cluster mesh and can therefore find sidecars that it wishes to assign configuration.

```
$ thanos config \
    --http-address     "0.0.0.0:9090" \
    --grpc-address     "0.0.0.0:9091" \
    --cluster.peers    "thanos-cluster.example.org" \
    --config           "promA.yaml;promB.yaml" \
```

The configuration supplied to the config component will then be distributed to the peers that config is aware of and exposed via a gRPC API so that each sidecar can get its configuration.
The gRPC endpoint will take a request from a sidecar and based on the sidecar making the request it will give back the computed configuration.

`thanos config` will initially only support a subset of the Prometheus Config will initially include a subset of global and scrape_config along with static_sd_config and file_sd_config.

The initial configuration strategy will be intended for a multi-tenant use case and so keep jobs with a given label together on a single Prometheus instance.

### Thanos Sidecar

With the addition of `thanos config` we will look to update the `thanos sidecar` to periodically get its configuration from the config component. Each time it does so it will update the Prometheus instance by either updating the targets via file_sd_config or adding a new configuration job and forcing a reload on the Prometheus configuration.

To do this the sidecar will need to know the location of the config component to get its configuration.

```
$ thanos sidecar \
    --tsdb.path        "/path/to/prometheus/data/dir" \
    --prometheus.url   "http://localhost:9090" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org" \
    --config.url       "thanos-cluster.config.org" \
```

### Client/Server Backwards/Forwards compatibility

This change would be fully backwards compatible, you do not have to use the config component within your Thanos setup and can still choose to manually set up configuration and service discovery for each node.

The alternatives below might be a good starting point for users that do not need to worry about re-allocation of targets when the number of Prometheus instances in a cluster scales.

## Alternatives considered

### Prometheus & Hashmod

An alternative to this is to use the existing [hashmod](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) functionality within Prometheus to enable [horizontal scaling](https://www.robustperception.io/scaling-and-federating-prometheus/), in this scenario a user would supply their entire configuaration to every Prometheus node and use hashing to scale out.
The main downside of this is that as a Prometheus instance is added or removed from the cluster the targets will be moved to a new scrape instance. This is bad if you are wanting to keep tenants to a minimal number of Prometheus instances.

### Prometheus & Consistent Hashing

[prometheus/prometheus - Issue 4309](https://github.com/prometheus/prometheus/issues/4309) looks at adding a consistent hashing algorithm to Prometheus which would allow us to minimise the re-allocation of targets as the scrape pool is scaled.
Unfortunately, based on discussions this does not look like it will be accepted and added to the Prometheus codebase.

## Related Future Work

### Flush WAL from Drained Prometheus

Once a Prometheus instance has been drained and no longer has targets to scrape we will wish to scale down and remove the instance. However, we will need to ensure that the data that is currently in the WAL block but not uploaded to object storage is flushed before we can remove the instance. Failing to do so will mean that any data in the WAL is lost when the Prometheus node is terminated.
During this flush period until it is confirmed that the WAL has been uploaded we should still have the Prometheus instance serve requests for the data in the WAL.

See [prometheus/tsdb - Issue 346](https://github.com/prometheus/tsdb/issues/346) for more information.
