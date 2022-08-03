---
type: Proposal
title: Granular Endpoint Configuration
status: Approved
owner: SrushtiSapkale
menu: proposals-accepted
---

* **Related Tickets:**

  * [Per-Store TLS configuration in Thanos Query](https://github.com/thanos-io/thanos/issues/977)

## What

This proposal builds up on the [Unified Endpoint Discovery Proposal](https://thanos.io/tip/proposals-accepted/202101-endpoint-discovery.md/) and [Automated, per-endpoint mTLS Proposal](https://thanos.io/tip/proposals-accepted/202106-automated-per-endpoint-mtls.md/) and to add possibility to have granular setting per endpoint and deprecate old flags.

## Why

The Thanos Querier component supports basic mTLS configuration for internal gRPC communication. Mutual TLS (mTLS) ensures that traffic is both secure and trusted in both directions between a client and server. This works great for basic use cases, but it still requires extra forward proxies to make it work for quite common deployments where different servers have different auth, cert or TLS options.
This is similar to cases where we want to have further per-endpoint configuration pieces like what APIs to enable or in future what load balancing groups it belongs to.
We also have to be prepared for more options that will appear as the functionality of Queriers grows per endpoint (e.g limits).

## Pitfalls of the current solution

Current users cannot use different certifcates, auth or TLS options for different endpoints. The current workarounds is to use layered queries for this which is much more complex and confusing deployment.
Second, quite viable option is to use forward proxy like envoy. The problem with this approach is that any Thanos adopter have to learn yet another configuration language/options, deployment (build, release, support) models and operational (metrics, migration) models  which is not trivial and hinders adoptions.

## Goals

* To add support for per-endpoint granularity configuration. For example:
  * TLS configuration in for Querier gRPC communication.
  * Basic/Token Auth
  * Strict mode
  * Enabled APIs (specifying that server which can serve Exemplars and Store should only be asked for Exemplars)
  * (perhaps more in future)
* Unify endpoint configuration. Which means deprecating all non `endpoint` flags for endpoint configuration like `rule`, `metadata`, `exemplar`, `store.sd-interval`, `store.sd-dns-interval`, `store.sd-dns-resolver`, `store.sd-files` and all `grpc-client-.*`.

## Non Goals

* Granular configuration & implementation for load balancing groups. This can be improved in the next proposals and is currently fulfilled using any forward or reverse proxy like `envoy`, `caddy` or `nginx`.

## How

A new CLI option `--endpoints.config`, with no dynamic reloading, which will accept the path to a yaml file is proposed which contains a list as follows:

```yaml
endpoints:
- tls_config:
    cert_file: ""
    key_file: ""
    ca_file: ""
    server_name: ""
  endpoints: []
  endpoints_sd_files:
    - files: []
  mode: ""
```

The YAML file contains set of endpoints (e.g Store API) with optional TLS options. To enable TLS either use this option or deprecated ones --grpc-client-tls* .

The endpoints in the list items with no TLS config would be considered insecure. Further endpoints supplied via separate flags (e.g. `--endpoint`, `--endpoint.strict`, `--endpoint.sd-files` (previous `--store.strict`, `--store.sd-files`)) will be considered secure only if TLS config is provided through `--secure`, `--cert`, `--key`, `--caCert`, `--serverName` cli options. User will only be allowed to use separate options or `--endpoints.config`.

If the mode is strict (i.e. `mode: ”strict”`) then all the endpoints in that list item will be considered as strict (statically configured store API servers that are always used, even if the health check fails, as supplied in `--endpoint.strict`). And if there is no mode specified (i.e `mode: “”`) then all endpoints in that item will be considered normal.

Also we noticed that even if the user wants to connect to the store api only, the store method indiscriminately returns all endpoints ie exemplar api, store api, etc, even if they do not expose store API which is confusing. This is because the query exposes endpoints of all the apis. We can update the filtering logic where the querier endpoint knows which api endpoints it should expose for a particular component.

## Alternatives

## Action Plan

* Implement `--endpoint.config`.
* Add e2e tests for the changes.
* Update the filtering of APIs in the querier to return only the endpoints that are needed.
