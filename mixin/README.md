# thanos-mixin

> Note that everything is experimental and may change significantly at any time. Also it still has missing alert and dashboard definitions for certain components, e.g. rule and sidecar. Please feel free to contribute.

This directory contains extensible and customizable monitoring definitons for Thanos. [Grafana](http://grafana.com/) dashboards, and [Prometheus rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) combined with documentation and scripts to provide easy monitoring experience for Thanos.

You can find more about monitoring-mixins in [the design document](https://github.com/monitoring-mixins/docs/blob/master/design.pdf), and you can check out other examples like [Prometheus Mixin](https://github.com/prometheus/prometheus/tree/master/documentation/prometheus-mixin).

The content of this project is written in [jsonnet](http://jsonnet.org/). This project could both be described as a package as well as a library.

## Requirements

### jsonnet

The content of this project consists of a set of [jsonnet](http://jsonnet.org/) files making up a library to be consumed.

We recommend to use [go-jsonnet](https://github.com/google/go-jsonnet). It's an implementation of [Jsonnet](http://jsonnet.org/) in pure Go. It is feature complete but is not as heavily exercised as the [Jsonnet C++ implementation](https://github.com/google/jsonnet).

To install:

```shell
go get github.com/google/go-jsonnet/cmd/jsonnet
```

### jsonnet-bundler

`thanos-mixin` uses [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler#install) (the jsonnet package manager) to manage its dependencies.

We also recommend you to use `jsonnet-bundler` to install or update if you decide to use `thanos-mixin` as a dependency for your custom configurations.

To install:

```shell
go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
```

> An e.g. of how to install a given version of this library: `jb install github.com/thanos-io/thanos/mixin@main`.

## Use as a library

To use the `thanos-mixin` as a dependency, simply use the `jsonnet-bundler` install functionality:

```shell
$ mkdir thanos-mixin; cd thanos-mixin
$ jb init  # Creates the initial/empty `jsonnetfile.json`
# Install the thanos-mixin dependency
$ jb install github.com/thanos-io/thanos/mixin@main # Creates `vendor/` & `jsonnetfile.lock.json`, and fills in `jsonnetfile.json`
```

To update the `thanos-mixin` as a dependency, simply use the `jsonnet-bundler` update functionality:

```shell
$ jb update
```

#### Configure

This project is intended to be used as a library. You can extend and customize dashboards and alerting rules by creating for own generators, such as the generators ([alerts.jsonnet](alerts.jsonnet) and [dashboards.jsonnet](dashboards.jsonnet)) that are use to create [examples](../examples). Default parameters are collected in [config.libsonnet](config.libsonnet), feel free to modify and generate your own definitions.

```libsonnet mdox-exec="cat mixin/config.libsonnet"
{
  local thanos = self,
  // TargetGroups is a way to help mixin users to add high level target grouping to their alerts and dashboards.
  // With the help of TargetGroups you can use a single observability stack to monitor several Thanos instances.
  // The key in the key-value pair will be used as "label name" in the alerts and variable name in the dashboards.
  // The value in the key-value pair will be used as a query to fetch available values for the given label name.
  targetGroups+:: {
    // For example for given following groups,
    // namespace: 'thanos_status',
    // cluster: 'find_mi_cluster_bitte',
    // zone: 'an_i_in_da_zone',
    // region: 'losing_my_region',
    // will generate queriers for the alerts as follows:
    //  (
    //     sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_failures_total{job=~"thanos-compact.*"}[5m]))
    //   /
    //     sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_total{job=~"thanos-compact.*"}[5m]))
    //   * 100 > 5
    //   )
    //
    // AND for the dashborads:
    //
    // sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_failures_total{cluster=\"$cluster\", namespace=\"$namespace\", region=\"$region\", zone=\"$zone\", job=\"$job\"}[$interval]))
    // /
    // sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_total{cluster=\"$cluster\", namespace=\"$namespace\", region=\"$region\", zone=\"$zone\", job=\"$job\"}[$interval]))
  },
  query+:: {
    selector: 'job=~".*thanos-query.*"',
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  queryFrontend+:: {
    selector: 'job=~".*thanos-query-frontend.*"',
    title: '%(prefix)sQuery Frontend' % $.dashboard.prefix,
  },
  store+:: {
    selector: 'job=~".*thanos-store.*"',
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receive+:: {
    selector: 'job=~".*thanos-receive.*"',
    title: '%(prefix)sReceive' % $.dashboard.prefix,
  },
  rule+:: {
    selector: 'job=~".*thanos-rule.*"',
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compact+:: {
    selector: 'job=~".*thanos-compact.*"',
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    selector: 'job=~".*thanos-sidecar.*"',
    thanosPrometheusCommonDimensions: 'namespace, pod',
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  bucketReplicate+:: {
    selector: 'job=~".*thanos-bucket-replicate.*"',
    title: '%(prefix)sBucketReplicate' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    timezone: 'UTC',
    selector: ['%s="$%s"' % [level, level] for level in std.objectFields(thanos.targetGroups)],
    dimensions: ['%s' % level for level in std.objectFields(thanos.targetGroups)],

    overview+:: {
      title: '%(prefix)sOverview' % $.dashboard.prefix,
      selector: std.join(', ', thanos.dashboard.selector),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
}
```

You can format your code using:

```shell
$ make jsonnet-format
```

## Examples

This project is intended to be used as a library. However, it also provides drop-in examples to monitor Thanos.

### Requirements

Although all the required dependencies are handled by `Makefile`, keep in mind that in addition the dependencies that are listed above we have following dependencies:

#### gojsontoyaml

`gojsontoyaml` is used to convert generated `json` definitions to `yaml`.

To install:

```shell
go get github.com/brancz/gojsontoyaml
```

### Generate

To generate examples after modifying, make sure `jsonnet` dependencies are installed.

```shell
$ make jsonnet-vendor
```

and then

```shell
$ make examples
```

Make action runs the jsonnet code, then reads each key of the generated json and uses that as the file name, and writes the value of that key to that file, and converts each json manifest to yaml.

> Make commands should handle dependencies for you.

### Test and validate

You validate your structural correctness of your Prometheus [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) or [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) with:

```shell
$ make example-rules-lint
```

Check out [test.yaml](../examples/alerts/tests.yaml) to add/modify tests for the mixin. To learn more about how to write test for Prometheus, check out [official documentation](https://www.prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).

You test alerts with:

```shell
$ make alerts-test
```

---
