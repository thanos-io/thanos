# Intermediate: (Bonus) Using Prometheus Agent for pure forward-only metric streaming with Thanos Receive

The [Thanos](thanos.io) project defines a set of components that can be composed together into a highly available metric system with **unlimited storage capacity** that **seamlessly** integrates into your existing Prometheus deployments.

But [Prometheus](https://prometheus.io/) project is far from slowing down the development. Together with the community, it constantly evolves to bring value to different network and cluster topologies that we see. Thanos brings distributed and cloud storage and querying to the table, built on the core Prometheus code and design. It allowed Prometheus to focus on critical collection and single cluster monitoring functionalities.

As we learned in the previous tutorial, certain situations require us to collect (pull) data from applications and stream them out of the cluster as soon as possible. `Thanos Receive` allows doing that by ingesting metrics using the Remote Write protocol that the sender can implement. Typically we recommended using Prometheus with short retention and blocked read and query API as a "lightweight" sender.

In November 2021, however, we, the Prometheus community, introduced a brand new Prometheus mode called "Agent mode". The implementation itself was already battle tested on https://github.com/grafana/agent, where it was available and authored by [Robert Fratto](https://github.com/rfratto) since 2020.

The agent mode is optimized for efficient metric scraping and forwarding (i.e. immediate metric removal once it's securely delivered to a remote location). Since this is incredibly helpful for the Thanos community, we wanted to give you first-hand experience deploying Prometheus Agent together with Thanos Receive in this course.

In this tutorial, you will learn:

* How to reduce Prometheus based client-side metric collection to a minimum, using the new Prometheus "Agent mode" with `Thanos Receiver`, explained in the previous tutorial.

> NOTE: This course uses docker containers with pre-built Thanos, Prometheus, and Minio Docker images available publicly.

### Prerequisites

Please complete tutorial #3 first: [Intermediate: Streaming metrics from remote source with Thanos Receive](https://www.katacoda.com/thanos/courses/thanos/3-receiver) 🤗

### Feedback

Do you see any bug, typo in the tutorial, or do you have some feedback for us?
Let us know on https://github.com/thanos-io/thanos or #thanos slack channel linked on https://thanos.io

### Contributed by:

* Bartek Plotka [@bwplotka](http://bwplotka.dev)
