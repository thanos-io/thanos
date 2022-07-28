---
type: proposal
title: Granular Endpoint Configuration
status: Approved
owner: SrushtiSapkale
menu: proposals-accepted
---

* **Related Tickets:**

  * [Per-Store TLS configuration in Thanos Query](https://github.com/thanos-io/thanos/issues/977)

## What

This proposal builds up on the [Unified Endpoint Discovery Proposal](https://thanos.io/tip/proposals-accepted/202101-endpoint-discovery.md/) and [Automated, per-endpoint mTLS Proposal](https://thanos.io/tip/proposals-accepted/202106-automated-per-endpoint-mtls.md/)

## Why

The Thanos Querier component supports basic mTLS configuration for internal gRPC communication. Mutual TLS (mTLS) ensures that traffic is both secure and trusted in both directions between a client and server. This works great for basic use cases but it still requires extra forward proxies to make it work for bigger deployments.

Let’s imagine we have an Observer Cluster that is hosting Thanos Querier along with Thanos Store Gateway. Within the same Observer Cluster, we would like to connect one or more Thanos Sidecars. Additionally, we also want to connect the Querier in the Observer Cluster to several remote instances of Thanos Sidecar in remote clusters.

In this scenario we can use a proxy server (e.g. envoy). In thanos as we have multiple queriers connected to a central querier, Envoy can efficiently loadbalance gRPC connections, something we currently don't support natively in Thanos. If we used the endpoints config to connect the global to the regional queriers, they will not be able to scale regional queriers due to the persistent nature of gRPC connections. We can use Envoy when we need to scale our regional queries but here having an endpoint configuration is a better option in distributed store architecture as Thanos can communicate securely and directly through remote endpoints.

## Pitfalls of the current solution

Ideally, we would want to use mTLS to encrypt the connection to the remote clusters. If we would enable the current mTLS, it would be applied to all the storeAPI’s but we don’t want it to be applied on the storeAPI’s of central Thanos instance (Observer cluster) in which Thanos query component is present (for faster communications with storeAPI’s (sidecars) of same cluster, to reduce the pain of provisioning certificates etc.). So it requires extra forward proxies to make it work for bigger (multi-cluster) deployments.

## Goals

* To add support for per-endpoint TLS configuration in Thanos Query Component for internal gRPC communication.
* Adding a new `endpoint.config` flag will deprecate the following flags: `store.sd-interval`, `store.sd-dns-interval`, `store.sd-dns-resolver`, `store.sd-files` and all `grpc-client-.*`. We can mark the current flags as deprecated, and after some time we can remove them.
* Enable token based auth for endpoints

## How

A new CLI option `--endpoints.config`, with no dynamic reloading, which will accept the path to a yaml file is proposed which contains a list as follows :

```yaml
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

* Granular endpoint configuration is not a solution for federate querier loadbalncing and scalability. As an alternative can embed Envoy in thanos for loadbalancing. This would work efficiently because endpointset cannot configure federated queriers when they scale due to persistent nature of grpc connections.

## Action Plan

* Implement `--endpoint.config`.
* Add e2e tests for the changes
* Update the filtering of APIs in the querier to return only the endpoints that are needed.
