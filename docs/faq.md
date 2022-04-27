# Frequently Asked Questions

## Querier

#### How does Thanos Query show the user the same metric values from the two Prometheus instances?

Querier is capable to deduplicate transparently metrics from two replicated sources like Prometheus replicas, replicated ingestion (Receivers) and more. This is called online deduplication. Read more about that [in Querier component docs](components/query.md).

There is also offline deduplication inside [Compactor Vertical Compaction](components/compact.md#vertical-compactions) logic that offers to deduplicate this data on rest if you want to reduce your storage size (make sure your object storage is available enough you can afford no back up of your data!). In this case Querier will have no duplicated data to deduplicate.

Querier also works well for mixed cases (some data is offline deduplicated, some data is not).

#### How does Thanos Query detect Prometheus anomalies when one of the Prometheus does not have the correct metrics?

It is not possible for Thanos to detect which Prometheus has "correct" metrics. What querier is doing is detecting cases where there was no metric scraped for some duration due to Prometheus being down, rolled or scrape job was temporarily failing in one replica. See details about algorithm in [Querier documentation](components/query.md).

If you misconfigure your ingestion or query federation by passing duplicated metric name, with all labels for semantically different metric collections, you will end up with inaccurate metric results. Fortunately this is actually quite hard to do within Prometheus ecosystem, since there are clear patterns to keep job/scrape target labels unique.

#### For over_time queries like avg_over_time, is the Thanos Querier query result correct even if each of the metrics data obtained from the redundant Prometheus contains outliers or missing values?

Yes, our [penalty-based deduplication](components/query.md#deduplication) tries to ensure consistent sample interval, while sticking to healthy replicas. Over time aggregations will be as accurate as any other aggregations.

#### Is Thanos Querier letting Prometheus do the query operations or does the query do operations itself?

> For example, when the query returns an average, does Thanos Querier get raw data from Prometheus and calculate the average or Thanos Querier asks Prometheus to calculate it and simply return that data?

Querier mostly does the query itself. StoreAPI contract tells implementation to only serve raw data from sources like Prometheus (through sidecar) and any other metric backends. The main PromQL computation is performed on the top level by Querier, which allows true [Global View](components/query.md#use-case-1-global-view).

There is active work to enable query sharding and [pushdown](https://github.com/thanos-io/thanos/issues/305) capabilities that could work for Thanos users, but this is not trivial work. On top of that doing computation only in one type of components gives us more predictable computation characteristics (imagine Receive or Store Gateway requiring even more CPU and memory to handle incoming PromQL query requests).
