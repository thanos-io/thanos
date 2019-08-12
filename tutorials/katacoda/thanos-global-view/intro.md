In this scenario, we'll learn how to launch a Prometheus with a Thanos Sidecar and having a global view with Thanos Query.

## Thanos

Thanos is a set of components that can be composed into a highly available metric system with unlimited storage capacity, which can be added seamlessly on top of existing Prometheus deployments.

### Sidecar

The sidecar component of Thanos gets deployed along with a Prometheus instance. This allows sidecar to optionally upload metrics to object storage and allow Queriers to query Prometheus data with a dedicated common, efficient StoreAPI.

### Query

The query component implements the Prometheus HTTP v1 API to query data in a Thanos cluster via PromQL. This allows compatibility with Grafana or other consumers of Prometheus' API.