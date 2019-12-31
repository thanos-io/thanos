# Dashboards

There exists Grafana dashboards for each component (not all of them complete) targeted for environments running Kubernetes:

- [Thanos Overview](thanos-overview.json)
- [Thanos Compact](thanos-compactor.json)
- [Thanos Querier](thanos-querier.json)
- [Thanos Store](thanos-store.json)
- [Thanos Receiver](thanos-receiver.json)
- [Thanos Sidecar](thanos-sidecar.json)
- [Thanos Ruler](thanos-ruler.json)

You can import them via `Import -> Paste JSON` in Grafana.
These dashboards require Grafana 5 or above, importing them in older versions are known not to work.

## Configuration

All dashboards are generated using [`thanos-mixin`](https://github.com/thanos-io/thanos/tree/master/mixin/thanos) and check out [README](https://github.com/thanos-io/thanos/tree/master/mixin/thanos/README.md) for further information.
