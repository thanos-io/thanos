

> NOTE: Click `Copy To Editor` for each config to propagate the configs to each file.

What if we can have one single point of entry in front of Queries instead of separate Queriers? And by doing so slice and dice our queries depend on the time and distribute them between queriers to balance the load? Moreover, why not cache these responses so that next time someone asks for the same time range we can just serve it from memory. Wouldn't it be faster?

Yes, we can do all these using Thanos Query Frontend. Let's see how we can do it.

### First let's deploy a nginx proxy to simulate latency

We are running this tutorial on a single machine in this setup, as a result it's really hard to observe latencies that you would normally experience in a real-life setups. In order to simulate a real-life latency we are going to put a proxy in front of our Thanos Querier.

For that let's setup a nginx instance:

<pre class="file" data-filename="nginx.conf" data-target="replace">
server {
 listen 10902;
 server_name proxy;
 location / {
  echo_exec @default;
 }
 location ^~ /api/v1/query_range {
     echo_sleep 1;
     echo_exec @default;
 }
 location @default {
     proxy_pass http://127.0.0.1:10912;
 }
}
</pre>

```
docker run -d --net=host --rm \
    -v $(pwd)/nginx.conf:/etc/nginx/conf.d/default.conf \
    --name nginx \
    yannrobert/docker-nginx && echo "Started Querier Proxy!"
```{{execute}}

### Verify

Let's check if it's running!

```
docker ps
```{{execute}}

## Deploy Thanos Query Frontend

First, let's create necessary cache configuration for Frontend:

<pre class="file" data-filename="frontend.yml" data-target="replace">
type: IN-MEMORY
config:
  max_size: "0"
  max_size_items: 2048
  validity: "6h"
</pre>

And deploy Query Frontend:

```
docker run -d --net=host --rm \
    -v $(pwd)/frontend.yml:/etc/thanos/frontend.yml \
    --name query-frontend \
    quay.io/thanos/thanos:v0.19.0 \
    query-frontend \
    --http-address 0.0.0.0:20902 \
    --query-frontend.compress-responses \
    --query-frontend.downstream-url=http://127.0.0.1:10902 \
    --query-frontend.log-queries-longer-than=5s \
    --query-range.split-interval=1m \
    --query-range.response-cache-max-freshness=1m \
    --query-range.max-retries-per-request=5 \
    --query-range.response-cache-config-file=/etc/thanos/frontend.yml \
    --cache-compression-type="snappy" && echo "Started Thanos Query Frontend!"
```{{execute}}

### Setup Verification

Once started you should be able to reach the Querier, Query Frontend and Prometheus.

* [Prometheus](https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com/)
* [Querier](https://[[HOST_SUBDOMAIN]]-10902-[[KATACODA_HOST]].environments.katacoda.com/)
* [Query Frontend](https://[[HOST_SUBDOMAIN]]-20902-[[KATACODA_HOST]].environments.katacoda.com/)

Now, go and execute a query on [Querier](https://[[HOST_SUBDOMAIN]]-10902-[[KATACODA_HOST]].environments.katacoda.com/) and observe the latency.
And then go and execute the same query on [Query Frontend](https://[[HOST_SUBDOMAIN]]-20902-[[KATACODA_HOST]].environments.katacoda.com/).
For the fist execution you will observe that the query execution takes longer than the query on Querier.
That's because we have an nginx proxy between Query Frontend and Querier.

Now if you execute the same query again on Query Frontend for the same time frame using time selector in graph section in the UI (time is always shifting).
See that it's much faster?
It's taking much less time because we are just serving the response from the cached results.

Good! You've done it!
