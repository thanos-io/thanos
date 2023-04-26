---
type: proposal
title: Tenancy awareness in query path
status: accepted
owner: douglascamata
menu: proposals-accepted
---

## Tenancy awareness in query path

* **Owners:**
    * [@douglascamata](github.com/douglascamata)

* **Related Tickets:**
    * [Querier: Added native multi-tenancy support](https://github.com/thanos-io/thanos/pull/4141).
    * [Proposal: Native Multi-tenancy Support](https://github.com/thanos-io/thanos/pull/4055)

* **Other docs:**
    * `<Links…>`

<!--
> TL;DR: Give a summary of what this document is proposing and what components it is touching.
>
> *For example: This design doc is proposing a consistent design template for “example.com” organization.*
-->
 
This design doc proposes to add tenancy awareness in the query path. 

## Why

<!--
Provide a motivation behind the change proposed by this design document, give context.

*For example: It’s important to clearly explain the reasons behind certain design decisions in order to have a consensus between team members, as well as external stakeholders. Such a design document can also be used as a reference and knowledge-sharing purposes. That’s why we are proposing a consistent style of the design document that will be used for future designs.*
-->

In a multi-tenant environment, it's important to be able to identify which tenants are experiencing issues and configure (e.g. with different limits) each one of them individually and according to their usage of the platform so that the quality of service can be guaranteed to all the tenants. 


### Pitfalls of the current solution

<!--
What specific problems are we hitting with the current solution? Why it’s not enough?

*For example, We were missing a consistent design doc template, so each team/person was creating their own. Because of inconsistencies, those documents were harder to understand, and it was easy to miss important sections. This was causing certain engineering time to be wasted.*
-->

The current lack of tenancy awareness in Thanos' query path makes it impossible to investigate issues related to multi-tenancy without the use of external tools or proxies. For example, it's impossible to determine which tenants are experiencing high latency or high error rates.

## Goals

<!--
Goals and use cases for the solution as proposed in [How](#how):

* Allow easy collaboration and decision making on design ideas.
* Have a consistent design style that is readable and understandable.
* Have a design style that is concise and covers all the essential information.
-->

* Allow the query path components to be configurable to identify tenants, opening the way to the implementation per-tenant features on the query path. These features include, but aren't limited to, the following:
  * Per-tenant observability.
  * Per-tenant settings. For example, having different limits per tenant, which is a common request. 
  * Enforce presence of one or more tenant labels in queries.

### Audience

<!--
If this is not clear already, provide the target audience for this change.
-->

Any team running Thanos in a multi-tenant environment.

## Non-Goals

<!--
* Move old designs to the new format.
* Not doing X,Y,Z.
-->

* Add multi-tenancy to Thanos Ruler. It's not needed. One will still be able to point their Ruler to the multi-tenant Query Frontend, effectively making it multi-tenant. 
* Implement cross-tenant querying. It poses the question about how to track metrics of a multi-tenant query, probably requiring new logic for query splitting and/or separation of the concepts of query initiator tenant and query target tenants. This is out of scope and an specific proposal can be created for it.

## How

<!--
Explain the full overview of the proposed solution. Some guidelines:

* Make it concise and **simple**; put diagrams; be concrete, avoid using “really”, “amazing” and “great” (:
* How you will test and verify?
* How you will migrate users, without downtime. How we solve incompatibilities?
* What open questions are left? (“Known unknowns”)
-->

* Implement a command line argument in the Query Frontend and Sidecar components that allows them to identify the tenant that is making the request. The example of Thanos Receive is a good starting point for this: it uses the `--receive.tenant-label-name="tenant_id"` flag to identify the tenant label and we can standardize on it. The default behavior is to not identify tenants, preserving backwards compatibility. 
* Implement a mechanism to allow incoming requests to specify the tenant being queried in Query Frontend and Sidecar. Both an HTTP header or an URL param should be allowed methods. With the URL parameter we ensure the system is compatible with Grafana datasource definition and with the HTTP header it can be integrated more gracefully with other upstream projects. Again, we follow the example of Thanos Receive, which uses the `--receive.tenant-header="THANOS-TENANT"` flag to configure the tenant header (and URL param, in this case).
* The tenant information from a given request should travel downstream to all the components being called through the HTTP header, so that it can be added to their metrics, traces, and logs without requiring duplicated/extra work to re-parse the query. This applies even to gRPC calls, so that the propagation of the tenant information reaches the Thanos Store.
* The label verification and enforcement should be done by reusing prom-label-proxy's [Enforce.EnforceMatchers](https://github.com/prometheus-community/prom-label-proxy/blob/main/injectproxy/enforce.go#L141). There's no reason to (re)implement something specific and special for Thanos.
* Update metrics exported by the components in the query path to include the tenant label when it's available. 
* Implement a tenant selector in the Query Frontend UI, which should communicate the tenant to Query Frontend using the HTTP header.

## Alternatives

<!--
The section stating potential alternatives. Highlight the objections reader should have towards your proposal as they read it. Tell them why you still think you should take this path [[ref](https://twitter.com/whereistanya/status/1353853753439490049)]

1. This is why not solution Z...
-->

### Alternative implementations

1. Apply verification and enforcement logic in Querier instead of Query Frontend.

It initially seems like a better idea, but we might still want some components to skip the tenant label verification, like the Ruler. Plus on big queries where the Query Frontend splits them into multiple smaller queries there would be a lot of extra work done to verify and enforce the tenant label in each one of them.

### Alternative solutions

1. Use a proxy to enforce and identify tenants in queries.

While this could work for some of the features, like exporting per-tenant metrics, it would have to be inserted in front of many different components. Meanwhile, it doesn't solve the requirement for per-tenant configuration.

2. Use a separate query path for each tenant.

This incurs in a lot of wasted resources and demands manual work, unless a central tenant configuration is used and a controller is built around it to automatically manage the query paths.

## Action Plan

<!--
The tasks to do in order to migrate to the new idea.

* [ ] Task one <gh issue="">

* [ ] Task two <gh issue=""> ...
-->