# Thanos Service Discovery

Service discovery has a vital place in Thanos components. It allows Thanos to discover different set API targets required to perform certain operations.
This logic is meant to replace Gossip that [is planned to be removed.](/docs/proposals/approved/201809_gossip-removal.md)

Currently places that uses Thanos SD:
* `Thanos Query` needs to know about [StoreAPI](https://github.com/improbable-eng/thanos/blob/d3fb337da94d11c78151504b1fccb1d7e036f394/pkg/store/storepb/rpc.proto#L14) servers in order to query metrics from them.
* `Thanos Rule` needs to know about `QueryAPI` servers in order to evaluate recording and alerting rules.
* (Only static option with DNS discovery): `Thanos Rule` needs to know about `Alertmanagers` HA replicas in order to send alerts.

Currently there are several ways to configure this and they are described below in details:

* Static Flags
* File SD
* DNS SD

## Static Flags

The simplest way to tell a component about a peer is to use a static flag.

### Thanos Query

The repeatable flag `--store=<store>` can be used to specify a `StoreAPI` that `Thanos Query` should use.

### Thanos Rule

The repeatable flag `--query=<query>` can be used to specify a `QueryAPI` that `Thanos Rule` should use.

The repeatable flag `--alertmanager.url=<alertmanager>` can be used to specify a `Alertmanager API` that `Thanos Rule` should use.

## File Service Discovery

File Service Discovery is another mechanism for configuring components. With File SD, a
list of files can be watched for updates, and the new configuration will be dynamically loaded when a change occurs.
The list of files to watch is passed to a component via a flag shown in the component specific sections below.

The format of the configuration file is the same as the one used in [Prometheus' File SD](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config).
Both YAML and JSON files can be used. The format of the files is this:

* JSON:
```json
[
  {
    "targets": ["localhost:9090", "example.org:443"]
  }
]
```

* YAML:
```yaml
- targets: ['localhost:9090', 'example.org:443']
```

As a fallback, the file contents are periodically re-read at an interval that can be set using a flag specific for the component and shown below.
The default value for all File SD re-read intervals is 5 minutes.

### Thanos Query

The repeatable flag `--store.sd-files=<path>` can be used to specify the path to files that contain addresses of `StoreAPI` servers.
The `<path>` can be a glob pattern so you can specify several files using a single flag.

The flag `--store.sd-interval=<5m>` can be used to change the fallback re-read interval from the default 5 minutes.

### Thanos Rule

The repeatable flag `--query.sd-files=<path>` can be used to specify the path to files that contain addresses of `QueryAPI` servers.
Again, the `<path>` can be a glob pattern.

The flag `--query.sd-interval=<5m>` can be used to change the fallback re-read interval.

## DNS Service Discovery

DNS Service Discovery is another mechanism for finding components that can be used in conjunction with Static Flags or File SD.
With DNS SD, a domain name can be specified and it will be periodically queried to discover a list of IPs.

To use DNS SD, just add one of the following prefixes to the domain name in your configuration:

* `dns+` - the domain name after this prefix will be looked up as an A/AAAA query. *A port is required for this query type*.
An example using this lookup with a static flag:
```
--store=dns+stores.thanos.mycompany.org:9090
```

* `dnssrv+` - the domain name after this prefix will be looked up as a SRV query. You do not need to specify a port as the
one from the query results will be used. An example:
```
--store=dnssrv+_thanosstores._tcp.mycompany.org
```

The default interval between DNS lookups is 30s. You can change it using the `store.sd-dns-interval` flag for `StoreAPI`
configuration in `Thanos Query`, or `query.sd-dns-interval` for `QueryAPI` configuration in `Thanos Rule`.

## Other

Currently, there are no plans of adding other Service Discovery mechanisms like Consul SD, kube SD, etc. However, we welcome
people implementing their preferred Service Discovery by writing the results to File SD which will propagate them to the different Thanos components.
