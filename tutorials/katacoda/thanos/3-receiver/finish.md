# Summary

Congratulations! 🎉🎉🎉
You completed this `Thanos Receive` tutorial. Let's summarize what we learned:

* `Thanos Receive` is a component that implements the `Prometheus Remote Write` protocol.
* Prometheus can be configured to remote-write its metric data in real-time to another server that implements the `Remote Write` protocol.
* Thanos Receive allows us to ingest the data from sources which have limited accessibility, or that has no querying / storing capabilities and they have to forward data somewhere else (e.g Prometheus in Agent mode explained in next tutorial).

See next courses for other tutorials about different deployment models and more advanced features of Thanos!

## Further Reading

To understand more about `Thanos Receive` - check out the following resources:
* [Thanos Receive Documentation](https://thanos.io/tip/components/receive.md/)
* [Thanos Receive Design Document](https://thanos.io/tip/proposals/201812_thanos-remote-receive.md/)
* [Pros/Cons of allowing remote write in Prometheus](https://docs.google.com/document/d/1H47v7WfyKkSLMrR8_iku6u9VB73WrVzBHb2SB6dL9_g/edit#heading=h.2v27snv0lsur)

### Feedback

Do you see any bug, typo in the tutorial or you have some feedback for us?

let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io