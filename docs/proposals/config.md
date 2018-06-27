# Thanos Cluster Configuration

Status: **draft** | in-review | rejected | accepted | complete 

Implementation Owner: [@domgreen](https://github.com/domgreen)

## Motivation

Currently, each scraper manages their own configuration via [Prometheus Configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) which contains information about the [scrape_config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#%3Cscrape_config%3E) and targets that the scraper will be collecting metrics from.

As we start to dynamically scale the collection of metrics (new Prometheus instances) or increase the targets for a current tenant we wish to keep the collection of metrics to a consistent node and not re-allocate shards to other scrapers. 

This approach is not intended to replace current horizontal scaling strategies in Prometheus but enable new strategies for environments where the number of targets can be highly dynamic and require spinning up and removing scrapers based on the workload. Typically most Prometheus setus would have a static number of targets and therefore able to use `hashmod` within Prometheus. If adding a new scraper infrequent churn from each Prometheus TSDB would be reduced and not impact the system.
We have seen the `hashmod` scaling approach have a problem with hot shards in the past whereby a given Prometheus instance may endup being overloaded by the number or amount of data from its targets and is difficult to redistribute the load onto other Promethus instances in the scrape pool.


## Goals

The primary goal of having a specific `config` component are:

- enable auto-scaling of scrapers to meet the needs of dynamic targets
- Opt-in approach users can and should use existing scaling if it works for their use case

Secondary goals of the proposal would be:

- reduce churn on the underlying Prometheus TSDB
- multi-tenant aware, query minimal scrapers for data
- reduce hot sharding issues

## Proposal

As we scale out horizontally we wish to manage configuration such as target assignment to scrapes in a more intelligent way, firstly by keeping targets on the same scrape instance as much as possible as we scale the scrape pool and in the future more intelligent bin packing of targets onto scrapers.

We would look to add a new component to the Thanos system `thanos config` that would be a central point for loading configuration for the entire cluster and advertising via APIs the configuration for each sidecar.

The `thanos config` will have a configuration endpoint that each `thanos sidecar` will call into to get their own scrape_config jobs along with their targets. Once the sidecar has its jobs it will be able to update targets / scrape_config for the Prometheus instance it is running alongside. This instance can then be reloaded or automatically pick up new targets via file_sd_config.

The config component will keep track of what sidecar's are in the cluster at any time so will be able to have a central view of where targets / labels are being scraped from. Therefore, as a new scrape instance comes online it join the gossip cluster and therefoe config will know that it can start assigning configuration to this new node.

```
      ┌─ reload ─┐               ┌─ reload ─┐
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

The configuration supplied to the config component will then be distributed to the peers that config is aware of and exposed via a gRPC API so that each sidecar can get its configuarion.
The gRPC endponit will take a request from a sidecar and based on the sidecar making the request it will give back the computed configuration.

`thanos config` will initially only support a subset of the Prometheus Config will initially include a subset of global and scrape_config along with static_sd_config and file_sd_config.

The inital configuration strategy will be intended for a multi-tenant use case and so keep jobs with a given label together on a single Prometheus instance.

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

### Prometheus & Consistant Hashing

[Issue 4309](https://github.com/prometheus/prometheus/issues/4309) looks at adding a consistant hashing algorith to Promethus which would allow us to minimise the re-allocation of targets as the scrape pool is scaled. 
Unfortunatley, based on discussions this does not look like it will be accepted and added to the Prometheus codebase.
