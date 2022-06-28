# Multi-Tenancy

Thanos supports multi-tenancy by using external labels. For such use cases, the [Thanos Sidecar](../components/sidecar.md) based approach with layered [Thanos Queriers](../components/query.md) is recommended.

You can also use the [Thanos Receiver](../components/receive.md) however, we don't recommend it to achieve a global view of data of a single-tenant. Also note that, multi-tenancy may also be achievable if ingestion is not user-controlled, as then enforcing of labels, for example using the [prom-label-proxy](https://github.com/openshift/prom-label-proxy) (please thoroughly understand the mechanism if intending to employ this mechanism, as the wrong configuration could leak data).

Thanos supports an optional whitelist (see `--receive.tenant-whitelist` flag) to strongly enforce that only a specific list of tenants can send to this receive instance. In combination with other tools that specify the tenant header or using `--receive.tenant-certificate-field` to extract a tenant value from a given client TLS certificate, Thanos can fully enforce that only permitted Prometheus instances can send data into the Thanos receiver.
