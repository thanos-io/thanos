## At Red Hat we help to maintain many open source projects.

One of such project is the https://github.com/coreos/prometheus-operator, first project that
started leveraging CRDs to operate stateful resources like Prometheus.

Thanos, while mostly built from stateless components, is well integrated with Prometheus Operator.

This quick demo will show you how to deploy Prometheuses with Thanos via our Operator for:

* Seamless ingestion HA support. 💣
* Global queries, alerts and recording rules. 🌎
* Long Term storage support with object storage. 📦📦📦
* Centralized storage using remote write (streaming). ⚡
