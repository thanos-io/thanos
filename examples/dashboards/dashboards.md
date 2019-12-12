# Dashboards

There exists Grafana dashboards for each component (not all of them complete) targeted for environments running Kubernetes:

- [Thanos Overview](thanos-overview.json)
- [Thanos Compact](thanos-compact.json)
- [Thanos Query](thanos-querier.json)
- [Thanos Store](thanos-store.json)
- [Thanos Receive](thanos-receiver.json)
- [Thanos Sidecar](thanos-sidecar.json)
- [Thanos Rule](thanos-rule.json)

You can import them via `Import -> Paste JSON` in Grafana.
These dashboards require Grafana 5 or above, importing them in older versions are known not to work.

## Configuration

All dashboards are generated using [`thanos-mixin`](../../jsonnet/thanos-mixin) and check out [README](../../jsonnet/thanos-mixin/README.md) for further information.
