# Security Policy

At the Thanos team we are not security experts. However we try our best to avoid security concerns and to avoid writing features that handle sensitive information at all.

It's worth noting that we assume metric data to be sensitive and important. External labels and query API parameters are considered less sensitive, as they are logged and put into metrics/traces.

## What You CAN Expect:

* We follow best programming practices. We test heavily, including e2e tests against major object storages. We use vetting and static analysis tools on every pull request. We use secure protocols for building processes, e.g. when producing Docker images.
* We don't put any data that is stored in the TSDB into logs or instrumentation.
* If we use crypto tools, we always rely on FLOSS and standard libraries, like the official [Go crypt](https://golang.org/pkg/crypto/) library.
* We always use TLS by default for communication with all object storages.
* We use stable Go versions to build our images and binaries. We update Go as soon as a new version is released.
* We use only FLOSS tools.

## What We DON'T Do (yet):

* We don't encrypt metrics in local storage, i.e. on disk. We don't do client-side encryption for object storage. We recommend setting server-side encryption for object storage.
* We don't allow specifying authorization or TLS for Thanos server HTTP APIs.

## Supported Versions

| Version   | Supported          |
|-----------|--------------------|
| >= 0.10.1 | :white_check_mark: |
| < 0.10.1  | :x:                |

## Reporting a Vulnerability

If you encounter a security vulnerability, please let us know privately via the Thanos Team email address: thanos-io@googlegroups.com.
