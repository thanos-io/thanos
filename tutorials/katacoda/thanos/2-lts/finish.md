# Summary

Congratulations! 🎉🎉🎉
You completed our second Thanos tutorial. Let's summarize what we learned:

* To preserve the data beyond Prometheus regular retention time, we used an object storage system for backing up our historical data.
* The Thanos Store component acts as a data retrieval proxy for data inside our object storage.
* With Sidecar uploading metric blocks to the object store as soon as it is written to disk, it keeps the “scraper” (Prometheus with Thanos Sidecar), lightweight. This simplifies maintenance, cost, and system design.
* Thanos Compactor improved query efficiency and also reduced the required storage size.

See next courses for other tutorials about different deployment models and more advanced features of Thanos!

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?

let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io