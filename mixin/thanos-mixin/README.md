# thanos-mixin

> Note that everything is experimental and may change significantly at any time.
> Also it still has missing alert and dashboard definitions for certain components, e.g. rule and sidecar. Please feel free to contribute.

This directory contains extensible and customizable monitoring definitons for Thanos. [Grafana](http://grafana.com/) dashboards, and [Prometheus rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) combined with documentation and scripts to provide easy monitoring experience for Thanos.

You can find more about monitoring-mixins in [the design document](https://docs.google.com/document/d/1A9xvzwqnFVSOZ5fD3blKODXfsat5fg6ZhnKu9LK3lB4/edit#heading=h.gt9r2h2gklj3), and you could check out other examples like [Prometheus Mixin](https://github.com/prometheus/prometheus/tree/master/documentation/prometheus-mixin).

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

Install this library in your own project with [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler#install) (the jsonnet package manager):

To install:

```shell
go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
```

> An e.g. of how to install a given version of this library: `jb install github.com/thanos-io/thanos/mixin/thanos-mixin@master`.

## Use as a library

To use the `thanos-mixin` as a dependency, simply use the jsonnet-bundler install functionality:
```shell
$ mkdir thanos-mixin; cd thanos-mixin
$ jb init  # Creates the initial/empty `jsonnetfile.json`
# Install the thanos-mixin dependency
$ jb install github.com/thanos-io/thanos/mixin/thanos-mixin@master # Creates `vendor/` & `jsonnetfile.lock.json`, and fills in `jsonnetfile.json`
```

To update the `thanos-mixin` as a dependency, simply use the jsonnet-bundler update functionality:
```shell
$ jb update
```

#### Configure

This project is intended to be used as a library. You can extend and customize dashboards and alerting rules by creating for own generators, such as the generators ([alerts.jsonnet](alerts.jsonnet) and [dashboards.jsonnet](dashboards.jsonnet)) that are use to create [examples](examples). As a convention shared variables are collected in [config.jsonnet](config.jsonnet), feel free to modify and generate your own definitons.

[embedmd]:# (config.libsonnet)
```libsonnet
{
  _config+:: {
    thanosQueryJobPrefix: 'thanos-querier',
    thanosStoreJobPrefix: 'thanos-store',
    thanosReceiveJobPrefix: 'thanos-receive',
    thanosRuleJobPrefix: 'thanos-rule',
    thanosCompactJobPrefix: 'thanos-compact',
    thanosSidecarJobPrefix: 'thanos-sidecar',

    thanosQuerySelector: 'job=~"%s.*"' % self.thanosQueryJobPrefix,
    thanosStoreSelector: 'job=~"%s.*"' % self.thanosStoreJobPrefix,
    thanosReceiveSelector: 'job=~"%s.*"' % self.thanosReceiveJobPrefix,
    thanosRuleSelector: 'job=~"%s.*"' % self.thanosRuleJobPrefix,
    thanosCompactSelector: 'job=~"%s.*"' % self.thanosCompactJobPrefix,
    thanosSidecarSelector: 'job=~"%s.*"' % self.thanosSidecarJobPrefix,

    // We build alerts for the presence of all these jobs.
    jobs: {
      ThanosQuerier: $._config.thanosQuerySelector,
      ThanosStore: $._config.thanosStoreSelector,
      ThanosReceive: $._config.thanosReceiveSelector,
      ThanosRule: $._config.thanosRuleSelector,
      ThanosCompact: $._config.thanosCompactSelector,
      ThanosSidecar: $._config.thanosSidecarSelector,
    },

    // Config for the Grafana dashboards in the thanos-mixin
    grafanaThanos: {
      dashboardNamePrefix: 'Thanos / ',
      dashboardTags: ['thanos-mixin'],

      dashboardOverviewTitle: '%(dashboardNamePrefix)sOverview' % $._config.grafanaThanos,
      dashboardCompactTitle: '%(dashboardNamePrefix)sCompact' % $._config.grafanaThanos,
      dashboardQuerierTitle: '%(dashboardNamePrefix)sQuerier' % $._config.grafanaThanos,
      dashboardReceiveTitle: '%(dashboardNamePrefix)sReceive' % $._config.grafanaThanos,
      dashboardRuleTitle: '%(dashboardNamePrefix)sRule' % $._config.grafanaThanos,
      dashboardSidecarTitle: '%(dashboardNamePrefix)sSidecar' % $._config.grafanaThanos,
      dashboardStoreTitle: '%(dashboardNamePrefix)sStore' % $._config.grafanaThanos,
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

#### gojsontoyaml

`gojsontoyaml` is used to convert generated `json` definitons to `yaml`.

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

> Make commands should handle dependecies for you.

### Test and validate

You validate your structural correctness of your Prometheus [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) or [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) with:

```shell
$ make rules-lint
```

Check out [test.yaml](examples/alerts/tests.yaml) to add/modify tests for the mixin. To learn more about how to write test for Prometheus, check out [official documentation](https://www.prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).

You test alerts with:

```shell
$ make alerts-test
```

---
