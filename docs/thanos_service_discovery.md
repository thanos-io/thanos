# Thanos Service Discovery

Service discovery has a vital place in Thanos components that allows them to perform some logic against given set of APIs. 
Currently there are 2 places like this: 
* `Thanos Query` needs to know about [StoreAPI](https://github.com/improbable-eng/thanos/blob/d3fb337da94d11c78151504b1fccb1d7e036f394/pkg/store/storepb/rpc.proto#L14) servers in order to query metrics from them.
* `Thanos Rule` needs to know about `QueryAPI` servers in order to evaluate recording and alerting rules.

Currently there are several ways to configure this and they are described below.

## Static Flags
The simplest way to tell a component about a peer is to use a static flag.

### Thanos Query
The repeatable flag `--store=<store>` can be used to specify a `StoreAPI` that `Thanos Query` should use. 

### Thanos Rule
The repeatable flag `--query=<query>` can be used to specify a `QueryAPI` that `Thanos Rule` should use. 

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
Coming soon as part of both File SD and Static Flags. 

## Other
Currently, there are no plans of adding other Service Discovery mechanisms like Consul SD, kube SD, etc. However, we welcome 
people implementing their preferred Service Discovery by writing the results to File SD which will propagate them to the different Thanos components.
