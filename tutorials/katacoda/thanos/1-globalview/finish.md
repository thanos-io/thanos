# Summary

Congratulations! 🎉🎉🎉
You completed our very first Thanos tutorial. Let's summarize what we learned:

* The most basic installation of Thanos with Sidecars and Querier allows global view for Prometheus queries.
* Querier operates on `StoreAPI` gRPC API. It does not know if it's Prometheus, OpenTSDB, another Querier or any other storage, as long as API is implemented.
* With Thanos you can (and it's recommended to do so!) run multi-replica Prometheus servers. Thanos Querier `--query.replica-label` flag controls this behaviour.
* Sidecar allows to dynamically reload configuration for Prometheus and recording & alerting rules in Prometheus.

See next courses for other tutorials about different deployment models and more advanced features of Thanos!

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io
