# Objmeta

***NOTE:** This component is only needed when your thanos workload is very large, how large is large? when the number of blocks in object storage is more than 100,000 or the HEAD request to object storage more than 10k per second.

The `thanos objmeta` command runs a component that manage the thanos block metadata stored in object storage, allow fast iter all the block and get block metadata.

In details:

* It wrapped Thanos `objstore.Bucket` API, when set a object name like `{blockID}/meta.json` `{blockID}/deletion-mark.json` `{blockID}/no-compact-mark.json`, is call the objmeta component to save this metadata additional.
* When get object name is a metadata file, it get from metadata storage backend directly with high performance, save the cost from object storage.
* It will sync from object storage when first start.

## metadata storage backend

### Redis

We recommend you are known the [redis persistence practise](https://redis.io/docs/management/persistence/), use RBD + AOF persistence option.

### Parquet file in object storage

(TODO)

## Example deployment

```bash
thanos objmeta \
  --objstore.config-file=/etc/thanos/thanos_bucket_config.yaml \
  --objmeta.config-file=/etc/thanos/thanos_objmeta_redis.yaml
```

The example content of `thanos_bucket_config.yml`:

```yaml mdox-exec="go run scripts/cfggen/main.go --name=gcs.Config"
type: GCS
config:
  bucket: ""
  service_account: ""
prefix: ""
```

The example content of `thanos_objmeta_redis.yaml`:

```yaml
type: Redis
config:
  addr: redis-server:6379
  password: thanos
```

Then run other thanos component like store-gateway/compactor/sidecar/ruler/receiver, which has `--objstore.config` flag, should also add flag `--objmeta.endpoint=objmeta-addr:port`.

```bash
thanos store \
    --data-dir        "/local/state/data/dir" \
    --objstore.config-file "bucket.yml" \
    --objmeta.endpoint=objmeta-addr:port
```

## Flags

```$ mdox-exec="thanos objmeta --help"
usage: thanos objmeta [<flags>]

ObjMeta for Thanos object metadata.

Flags:
      --block-meta-fetch-concurrency=32
                                 Number of goroutines to use when fetching block
                                 metadata from object storage.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --grpc-server-max-connection-age=60m
                                 The grpc server max connection age. This
                                 controls how often to re-establish connections
                                 and redo TLS handshakes.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no
                                 client CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --log.level=info           Log filtering level.
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file'
                                 flag (mutually exclusive). Content of
                                 YAML file that contains object store
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object
                                 store configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.sync-interval=24h
                                 How often to sync from objstore
      --request.logging-config=<content>
                                 Alternative to 'request.logging-config-file'
                                 flag (mutually exclusive). Content
                                 of YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --request.logging-config-file=<file-path>
                                 Path to YAML file with request logging
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/logging.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file
                                 with tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --version                  Show application version.

```
