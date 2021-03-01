# Dashboards

There exists Grafana dashboards for each component (not all of them complete) targeted for environments running Kubernetes:

- [Thanos Overview](overview.json)
- [Thanos Compact](compact.json)
- [Thanos Querier](query.json)
- [Thanos Store](store.json)
- [Thanos Receiver](receive.json)
- [Thanos Sidecar](sidecar.json)
- [Thanos Ruler](rule.json)
- [Thanos Replicate](bucket-replicate.json)

You can import them via `Import -> Paste JSON` in Grafana.
These dashboards require Grafana 5 or above, importing them in older versions are known not to work.

## Configuration

All dashboards are generated using [`thanos-mixin`](https://github.com/thanos-io/thanos/tree/main/mixin) and check out [README](https://github.com/thanos-io/thanos/blob/main/mixin/README.md) for further information.
