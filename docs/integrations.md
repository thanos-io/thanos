---
title: Integrations
type: docs
menu: thanos
slug: /integrations.md
---

# Integrations 

## StoreAPI 

[StoreAPI](https://github.com/thanos-io/thanos/blob/master/pkg/store/storepb/rpc.proto) is a common proto interface for gRPC component that can connect to [Querier](components/query.md) in order to fetch the metric series. Natively Thanos implements [Sidecar](components/sidecar.md) (local Prometheus data), [Ruler](components/rule.md) and [Store gateway](components/store.md). This solves fetching series from Prometheus or Prometheus TSDB format, however same interface can be used to fetch metrics from other storages. 

Below you can find some public integrations with other systems through StoreAPI:

### OpenTSDB

[Geras](https://github.com/G-Research/geras) is an OpenTSDB integration service which can connect your OpenTSDB cluster to Thanos. Geras exposes the Thanos Storage  API, thus other Thanos components can query OpenTSDB via Geras, providing a unified  query interface over OpenTSDB and Prometheus.

Although OpenTSDB is able to aggregte the data, it's not supported by Geras at the moment. 
