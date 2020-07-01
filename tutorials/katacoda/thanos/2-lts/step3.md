# Thanos Store Gateway

In this step, we will learn about Thanos Store Gateway, how to start and what problems are solved by it.

## Thanos Components

Thanos is a single Go binary capable to run in different modes. Each mode represents a different component and can be invoked in a single command.

Let's take a look at all the Thanos commands:

```docker run --rm quay.io/thanos/thanos:v0.12.2 --help```

You should see multiple commands that solves different purposes, a block storage based long-term storage for Prometheus.

In this step we will focus on thanos `store gateway`:

```
  thanos store [<flags>]
    Store node giving access to blocks in a bucket provider
```



## Store Gateway/ Store :

* This component implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space.
* It joins a Thanos cluster on startup and advertises the data it can access.
* It keeps a small amount of information about all remote blocks on the local disk and keeps it in sync with the bucket.
This data is generally safe to delete across restarts at the cost of increased startup times.


You can read more about [Store](https://thanos.io/components/store.md/) here.

### TODO: Show that queries are working, served by Thanos Store Gateway.