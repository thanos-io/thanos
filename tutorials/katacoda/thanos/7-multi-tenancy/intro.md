# Advanced: Achieving Multi-Tenancy with Thanos

Welcome to the tutorial where you learn how to manage fruits 🍇 and vegetables 🥦 reliably and efficiently.

On serious note, we will look into how to use Thanos for multi-tenant use cases. Centralized, long-term
metrics capabilities is very addictive, so it's very often the case that multiple teams want to use Thanos without
impacting each other or accessing each other data.

Thanos was built with this in mind and it in this tutorial you will learn:

* Query Read **Hard-Tenancy**: How to setup it and what are the tradeoffs
* How to support 🍅 (seriously!) and reduce cost of infrastructure.
* Query Read **Soft-Tenancy**: How to setup it and what are the tradeoffs (feat: [prom-label-proxy](https://github.com/prometheus-community/prom-label-proxy))

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io

### Contributed by:

* Kemal [@kakkoyun](https://kakkoyun.me/)
* Bartek [@bwplotka](https://bwplotka.dev/)
