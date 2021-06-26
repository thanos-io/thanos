## Automated, per-endpoint mTLS

* **Owners:**

   * [@Namanl2001](https://github.com/Namanl2001)

* **Related Tickets:**

  * [Per-Store TLS configuration in Thanos Query](https://github.com/thanos-io/thanos/issues/977)

## What

The Thanos Querier component supports basic mTLS configuration for internal gRPC communication. This works great for basic use cases but it still requires extra forward proxies to make it work for bigger deployments. It’s also hard to rotate certificates automatically and configure safe mTLS. This proposal aims to allow a better TLS story.

## Why

Let’s imagine we have an Observer Cluster that is hosting Thanos Querier along with Thanos Store Gateway. In the same cluster we also have one or more Thanos Sidecars that we would like to connect to, within the cluster.

However, let's say we also need to connect the observer cluster’s querier to several remote instances of Thanos Sidecar in remote clusters. Ideally, we want to use mTLS to encrypt the connection to the remote clusters, but we don’t want to use it within the cluster. Mutual TLS (mTLS) ensures that traffic is both secure and trusted in both directions between a client and server.

In this scenario we need to use a proxy server (e.g. envoy). But, it would probably be simpler to do away with the proxies and let Thanos speak directly and securely with the remote endpoints.

### Pitfalls of the current solution

If we would enable the current mTLS, it would be applied to all the storeAPI’s but we don’t want it to be applied on the storeAPI’s of central Thanos instance (Observer cluster) in which Thanos query component is present (for faster communications with storeAPI’s (sidecars) of same cluster, to reduce the pain of provisioning certificates etc.) 

So it requires extra forward proxies to make it work for bigger (multi-cluster) deployments. Also currently there is no support for automatic client rotation in thanos.

## Goals

*   To add support for per-endpoint TLS configuration in Thanos Query Component for internal gRPC communication
*   Automatic certificate rotation without restart

## Non-Goals

*   TLS client support for HTTP 
*   TLS for gRPC clients other than Querier

## How

I would propose introducing a new CLI option `--endpoints.config`, with no dynamic reloading, which will accept the path to a yaml file which contains a list as follows:
```
- tls_config:
    cert_file: ""
    key_file: ""
    ca_file: ""
    server_name: ""
  endpoints: []
  endpoints_sd_files: []
  mode: ""
```
The endpoints in the list items with no TLS config as well as the those supplied via separate flags (e.g.  `--endpoint`, `--store.strict`, `--store.sd-files`) will be considered insecure. Further we would remove the `--secure`, `--cert`, `--key`, `--caCert`, `--serverName` cli _options from thanos query as they are already covered above inside `--endpoint.config`.

If the mode is strict (i.e. `mode: ”strict”`) then all the endpoints in that list item will be considered as strict (statically configured store API servers that are always used, even if the health check fails) currently supplied via `--store.strict` flag. And if there is no mode specified (i.e `mode: “”`) then all endpoints in that item will be considered normal.

**Automatic cert-rotation:** for this all the things are managed by the cert manager in the k8s cluster, we just need to redo the TLS handshake when the certificates are rotated by the cert-manager. And for this I would propose to add a new `--grpc-server-max-connection-age` CLI option which takes time as input and controls how often to re-do the tls handshake and re-read the certificates. This in reality is controlled by the keepalive’s [MaxConnectionAge](https://pkg.go.dev/google.golang.org/grpc/keepalive#ServerParameters) option (which will close the connection after every (e.g. 15m) inputted time duration) so when the connection is closed it requires a new tls handshake.

While reading the _cert_file_ on each handshake we can improve the performance by keeping track of the it’s modification time. We should re-load the certificates only if they are actually rotated and there is some change in their modification time otherwise returning the previously stored one. Also if the file is not modified recently then it could be found on the cache memory and it would be faster to get the file details (i.e. modification time).

## Alternatives

*   Try to close connection only when things are actually changed, not based on time.

## Action Plan

*   Implement `--endpoint.config`.
*   Implement gRPC certificate rotation as designed.
*   Add e2e tests for above changes.