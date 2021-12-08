# Logging

Thanos supports request logging via flags.

Components are either configured using `--request.logging-config-file` to reference to the configuration file or `--request.logging-config` to provide configuration as YAML directly.

## Configuration

Configuration can be supplied globally, which applies to both `grpc` and `http`. Alternatively `http` and/or `grpc` can be configured independently.

### Options

Valid `level` for `options` should be one of `INFO`, `WARNING`, `ERROR` or `DEBUG`.

```yaml
options:
  level: ERROR
```

The request logging is configurable per phase via the `decision`.

Valid decision combinations are as follows:

* Log details only when the request completes - Log level is determined by the status code from the response.

```yaml
options:
  decision:
    log_start: false
    log_end: true
```

* Log details prior to the request and when the request completes - Pre request log is made at `debug` level.

```yaml
options:
  decision:
    log_start: true
    log_end: true
```

### HTTP

Specifying an `http` block enables request logging for each endpoint.

```yaml
http:
  options:
    level: DEBUG
    decision:
      log_start: true
      log_end: true
```

The log level of the "post request" log is determined by the status code in the response.

The following mappings apply:
* `200 <= status code < 500` = `debug`
* `status code > 500` = `error`

Optionally, additional configuration can be supplied via the `config` block to create an allowlist of endpoint and port combinations. Note that this feature requires an exact match if enabled.

```yaml
http:
  config:
    - path: /api/v1/query
      port: 10904
    - path: /api/v1/app_range/metrics
      port: 3456
```

### gRPC

Specifying a `grpc` block enables request logging for each service and method name combination.

```yaml
grpc:
  options:
    level: DEBUG
    decision:
      log_start: true
      log_end: true
```

The log level of the "post request" log is determined by the [code](https://grpc.github.io/grpc/core/md_doc_statuscodes.html) in the response.

The following mappings apply:
* Codes `2, 12, 13 ,15` = `error`
* All other codes = `debug`

Optionally, additional configuration can be supplied via the `config` block to create an allowlist of service and method combinations. Note that this feature requires an exact match if enabled.

```yaml
grpc:
  config:
    - service: thanos.Store
      method: Info
```

## How to use `config` flags?

The following example shows how the logging config can be supplied to the `sidecar` component:

```yaml
      - args:
        - sidecar
        - |
          --objstore.config=type: GCS
          config:
            bucket: <bucket>
        - --prometheus.url=http://localhost:9090
        - |
          --request.logging-config=http:
            config:
              - path: /api/v1/query
                port: 10904
            options:
              level: DEBUG
              decision:
                log_start: true
                log_end: true
          grpc:
            config:
              - service: thanos.Store
                method: Info
            options:
              level: ERROR
              decision:
                log_start: false
                log_end: true
        - --tsdb.path=/prometheus-data
```

Note that in the above example, logs will be emitted at `debug` level. These logs will be filtered unless the flag `--log.level=debug` is set.
