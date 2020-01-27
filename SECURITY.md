# Security Policy

As the Thanos team we are not security experts. However we try our best to avoid security concerns or to avoid
writing features that handles sensitive information at all.

It's worth to note that we assume the metric data to be sensitive and important.
External labels and query API parameters are treated as less sensitive, as they are logged and put into metric/traces.

## What you CAN expect:

* We follow best programming practices. We test heavily including e2e tests against major object storages. We use vetting
and static analysis tool on every PR. We use secure protocols for building process (e.g to produce docker images)
* We don't log or put into our instrumentation any data that is stored in TSDB block.
* If we use crypto tools we always rely on FLOSS and standard libraries like official [Go crypt](https://golang.org/pkg/crypto/)
  library.
* We always use TLS by default for communication with all object storages.
* We use stable Go versions to build our images and binaries. We update Go version as soon as new one is released.
* We use only FLOSS tools.

## What we DON'T do (yet):

* We don't encrypt metric on local storage (e.g on disk). We don't do client encryption for object storage. We recommend
setting server side encryption for object storage.
* We don't allow to specify authorization or TLS for Thanos server HTTP APIs.

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.10.1   | :white_check_mark: |
| < 0.10.1   | :x:                |

## Reporting a Vulnerability

If you encounter security vulnerability, please let us know privately via Thanos Team email: thanos-io@googlegroups.com
