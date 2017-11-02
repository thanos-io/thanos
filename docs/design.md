# PromLTS

PromLTS is a set of components that can be composed into a highly available Prometheus setup with long term storage capabilities. Its main goals are operation simplicity and retaining of Prometheus's reliability properties.

Unlike existing solutions it does not attempt to integrate with the official yet experiemental remote APIs. This has a variety of reasons, which go beyond the scope of this document.
Instead its main integration point is the Prometheus 2.0 storage format ([spec][tsdb-format], [slides][tsdb-talk]) itself through the storage engine's [library][tsdb-lib].

## Architecture

### Shipping Data

```
┌────────────┬─────────┐                   ┌────────────┬─────────┐
│ Prometheus │ Sidecar │        ...        │ Prometheus │ Sidecar │
└────────────┴────┬────┘                   └────────────┴────┬────┘
                  │                                          │
                Blocks                                     Blocks
                  │                                          │
                  v                                          v
              ┌──────────────────────────────────────────────────┐
              │               Google Cloud Storage               │
              └──────────────────────────────────────────────────┘
```

At the foundation of PromLTS stands the vanilla Prometheus server itself. Integration into the overall systems happens through a sidecar. No changes to the general setup should be necessary.

Prometheus writes "blocks" of data, which represent all time series data collected during a fixed time window. A block is a simple directory with a few files:

```
01BX6V6TY06G5MFQ0GPH7EMXRH
├── chunks
│   ├── 000001
│   ├── 000002
│   └── 000003
├── index
├── meta.json
└── tombstones
```

A blocks top-level directory is a ULID (like UUID but lexicographically sortable and encoding the creation time).

* Chunk files hold a few hundred MB worth of chunks each. Chunks for the same series are sequentially aligned. Series in return are aligned by their metric name. This becomes relevant further down.
* The index file holds all information needed to lookup specific series by their labels and the positions of their chunks.
* `meta.json` holds meta information about a block like stats, time range, and compaction level.
* The tombstone file holds information about deleted series. They should be generally irrelevant for this project and can be ignored.

The sidecar will watch Prometheus's data directory and upload new blocks as they are created. Block files are uploaded to a single GCS bucket with the given directory structure. The tombstone file is omitted or applied before upload.
After successful upload the files may be deleted by the sidecar or be retained until Prometheus's own retention drops them.

This setup allows Prometheus nodes to keep functioning on their own. If the overall system fails, individual nodes can still collect data, evaluate queries, and send alerts.


### Accessing data

Data is now located in two places. The most recent data (~2h window) is on the Prometheus nodes themselves. Older data is stored in GCS. This requires a querying layer that can access and merge those data sets.
This is built as a stateless and horizontally scalable layer on top of Prometheus's [PromQL library][promql-lib]. It can serve queries for graphing and alerting alike.

Ability to merge data at query time also allows enables a unified view of data collected from an HA pair of identically configured Prometheus servers.

```
┌──────────────────────┐  ┌────────────┬─────────┐  ┌────────────┬─────────┐
│ Google Cloud Storage │  │ Prometheus │ Sidecar │  │ Prometheus │ Sidecar │
└─────────────────┬────┘  └────────────┴────┬────┘  └────────────┴─┬───────┘
                  │                         │                      │
                Chunks                    Chunks                Chunks
                  │                         │                      │
                  v                         v                      v
                ┌─────────────────────────────────────────────────────┐
                │                      Query layer                    │
                └─────────────────────────────────────────────────────┘
                         ^                  ^                  ^
                         │                  │                  │
                ┌────────┴────────┐  ┌──────┴─────┐       ┌────┴───┐
                │ Alert Component │  │ Dashboards │  ...  │ Web UI │
                └─────────────────┘  └────────────┘       └────────┘

```

We ideally want the query layer to treat GCS and Prometheus servers identical and talk to them via the same API (gRPC).

#### Query Layer <> Prometheus

The Sidecar implements the given API and serves them by transcoding data collected through Prometheus's [public query API][prom-http-api]. This is not the most efficient way but most likely fast enough.

#### Query Layer <> GCS

As block storage is generally slow, it is not feasible to always fetch data from it ad-hoc. Especially index searches, which have to sequentially read from multiple random positions in the index file to satisfy a query, would be very slow. Thus, an intermediate "store" layer is added, which then implements the same API as the sidecars.


### Store layer

The store layer is placed in front of GCS and resolves data queries. It additionally acts as a caching layer.

Index files for all blocks are loaded onto the disks of store layer instances to quickly resolve chunk positions without network round-trips. Chunks are then loaded via range requests from the chunk files in GCS.

Since chunks for the same series are sequential in the chunk files, mulitple chunks can be fetched with a single request, which mitigates the high latency. Similar consolidation may apply for chunks from related series, which are also co-located. Queried ranges from chunk files are held in an in-memory LRU cache.  


#### Sharding?

Index files account for an average of 6-8% of the total data size. A single node or single replica set, each loading all index files, can scale significantly.

**Example:**

Suppose a store node has 60TB of SSD disk space (64TB is GCP maximum). At 8% index size and 1.6 bytes/sample, this can hold index files for ~750TB of total time series data. At an average of 10 million active time series, scraped every 30 seconds, data for ~678 days can be stored.

There are straightforward methods to extending this time range by more than an order of magnitude:

* Delete old series for low-relevance data (e.g. number of running goroutines)
* Downsample data
* Make chunks for old data bigger (reduces indexed chunks)
* Only load index files for rarely accessed/very old data on demand

By current estimates, the CPU load for the index lookups and loading of chunks is expected to be relatively low. Thus there's no immediate need for sharding along this dimension either.

Overall, providing sharding is not of high priority for an initial version. However, it is overall possible with limited complexity and coordination since store needs are merely a persistent cache.

```
┌──────────────────────┐  ┌────────────┬─────────┐
│ Google Cloud Storage │  │ Prometheus │ Sidecar │
└─────────────────┬────┘  └────────────┴────┬────┘
                  │                         │
            Index/Chunks                  Chunks
                  │                         │
                  v                         │
                ┌──────────────┐            │
                │     Store    │            │
                └────────┬─────┘            │
                         │                  │
                       Chunks               │
                         │                  │
                         v                  v 
                ┌──────────────────────────────────┐
                │            Query layer           │
                └──────────────────────────────────┘

```

### Alerts and Recording Rules

TODO(fabxc), TL;DR:

* Have a hihgly available rule evaluator component which talks to the query layer and writes samples like a regular Prometheus server
* Treat it like a normal Prometheus server for serving chunks and shipping data to GCS
* Probably a good idea to define some scope rules, which then allow keeping alerting rules that don't span multiple Prometheus servers on the original collection servers. This retains the reliability model of Prometheus a bit.


### Re-processing old data

TODO(fabxc), TL;DR:

* There are many reason to touch data in GCS again: downsampling, deleting low-relevance data/dynamic retention, compacting blocks into larger ones, etc.
* A simple batch job that downloads, rewrites, and uploads blocks is perfectly sufficient

## Cost estimate

Suppose the previous example of 750TB of time series data. Particularly query and store instance types and count are a near-blind guess and will likely change.

* GCS
  * storage: $15,000/month
  * transfer: free within the zone
  * operations: most ops will be GET ($0.004 per 10k), still unknown but probably minor, assume $200/month for now (500 million ops)
* HA store node pair
  * 2 x 64TB provisioned SSD: $21,760
  * OR 2 x 64TB provisioned HDD: $5120
  * 2 x n1-standard-32: $1,552
* Query nodes:
  * 4 x n1-standard-16: $1,552

**Total: $39,834 (SSDs) or $23,194 (HDDs)**

Disk caching space for index files is a significant cost factor. It is probably reasonable to use HDDs as index files are fully mmaped. Hot access paths should be sufficiently cached, for cold lookups, the latency is likely still within bounds.

[tsdb-format]: https://github.com/prometheus/tsdb/tree/master/Documentation/format
[tsdb-talk]: https://www.slideshare.net/FabianReinartz/storing-16-bytes-at-scale-81282712
[tsdb-lib]: https://godoc.org/github.com/prometheus/tsdb
[promql-lib]: https://godoc.org/github.com/prometheus/prometheus/promql
[prom-http-api]: https://prometheus.io/docs/querying/api/