---
title: Tracking the tenant’s data usages by supporting multi-tenancy natively
type: proposal
menu: proposals
status: proposed
owner: Abhishek357
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/3835 (*: Flexible Multi-tenancy Data Model)
* https://github.com/thanos-io/thanos/issues/3834 (Multi tenant rule evaluation)
* https://github.com/thanos-io/thanos/issues/3822 (Ability to require tenant in queries)
* https://github.com/thanos-io/thanos/issues/3819 (Thanos receive per tenant limits)
* https://github.com/thanos-io/thanos/issues/3820 (Zone aware replication)
* https://github.com/thanos-io/thanos/issues/3572 (Track data usage across tenant / system)
* https://github.com/thanos-io/thanos/issues/1318 (*: Support multi-tenancy on object storage using custom prefix)
* https://github.com/thanos-io/thanos/pull/3289 (receiver/compact/store/sidecar: Added a provision in the bucket config to add a prefix)

### Summary

We want to introduce a special tenant label for identifying a tenant and make each component aware of it and then finally track the data usages across the tenants. The receiver is on the safe side as it has a tenant-specific label to uniquely identify tenants.

All the components implementing either the Store API (Sidecar, Ruler, Store Gateway, Receiver) or Query API (Thanos querier frontend and Thanos querier) will be affected. Currently, the ruler is exposing the store API but since there is an ongoing LFX project (to make the ruler stateless), it will upload the data to the receiver itself and it might no longer expose Store API.

### Motivation

* We actually don't have any way to track the resource usages of the query(like error rates, memory allocation, CPU usages along with the queries) and tenant-wise metrics.
* Every component should allow passing a special label that allows APIs that identify tenants.
* Currently, we use external labels to achieve multi-tenancy. These labels are sufficient for the sidecar (single-tenant per sidecar i.e hard tenant) but for other components (where we will have multiple tenants per component i.e soft tenancy) these external labels are not sufficient.

### Goals

1. Introduce a mechanism to identify tenants to support multi-tenancy use cases.
2. Isolate all the incoming query requests.
3. Track data usage across tenants.

#### Use Case

1. Allow admins to obtain tenant information on per tenant queries, operations, and ingestion.
2. Sending tenant-specific alerts to different alert managers.
3. Support cross tenant views.

### Non Goals

*  Not introduce any type of authorization/authenticatication mechanism.
    * We should deal with auth outside Thanos using some authentication proxy (eg. kube-rbac-proxy) allowing us to use any authN/authZ system we want.
    * Evaluate some set of rules and check if a certain label can access the data or not.

### Solution

* Introduce a new flag to enable or disable the multi-tenancy mode.
* When this flag is enabled we will modify the metrics which can be used to track usage characteristics of a tenant by injecting the tenant’s label (a const. Label ).
* We will make sure that the query request received has a tenant label and if not we have some default tenant label configured.
* We will enforce the tenant labels inside the Thanos code base(i.e we assume the authentication is done and the query has access to the tenant's data which it specifies) when we run components with a specified mode(i.e multi-tenancy flag is enabled). We will no longer require any label enforcing proxy.
* Support the cross tenant queries using regular expressions.
* Store API will advertise the tenant label and this how it will help us -
    * Let's assume a case of hard tenancy, we have two store-gateway advertising different sets of tenants.
    * Store-gateway - 1 advertises tenant-a, tenant-b.
    * Store-gateway - 2 advertises tenant-c, tenant-d.
    * For the query, we will have both the store-gateway, and depending on the query it will go to the desired store-gateway. Same goes for receive.
* So, when we get a query and we pass it, and we have a tenant label and we treat that as a special value for a tenant, we can extract that value and aggregate some metrics and inject these metrics into Thanos components itself. Then we have tenant-specific metrics. Depending on the data we collect from the Thanos we can generate the dashboard etc.
* Changes specific to components
    * Ruler
        * We have put some extra effort into Ruler, there will be rules according to different tenants and there will be a configuration layer from tenant to which alert manager(i.e for different tenants having different alert manager ) to redirect. We have tenant-id(tenant label) and we can inject them on all outgoing requests as header and querier knows how to deal with them, we get the queries done and if we get some alert fired, we will redirect to tenant-specific alert managers.
    * Store
        * We will build multi-tenancy on a label-based approach and after introducing this PR we will need a thin layer that will convert custom prefix to external label and the rest advertising label, querying will be the same.

#### Alternatives

1. If we want to track the resource usages of a query, we can deploy another X proxy(hypothetical) depending on the data we want to aggregate. Similar to prom-label proxy we can inject this proxy to each of thanos components and check the queries and depending on tenant label(or any specified label) we can aggregate the metrics, this would be okay and we can aggregate simple metrics like error rates, latencies. And then we generate metrics per tenant and scrape these proxies from another prometheus and then finally generate dashboard reports etc.
    * Though this approach is not sufficient because when we want to track actual resource usages like memory allocation, CPU usages along with the queries, then we need to get into the Thanos components itself and for this, we need to know about these tenant labels.

2. We can use a prom-label proxy for label enforcement.
    * It does not deal with cross-tenant queries. But with the proposed approach we can use regular expression for cross tenant queries.
    * Difficult to configure and increases complexity.

### Work plan

1. Fact check with Thanos code base if this implementation works and possible bottlenecks, or missing cases.
2. Check how interoperability with Cortex’s multi-tenancy could work, as some components of both projects already depend on each other.
3. Implement prom-label proxy functionality in Thanos codebase.
4. Collect useful metrics the Thanos exposes.