## Thanos Query Frontend

### Deploy Thanos Query Frontend

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
    quay.io/thanos/thanos:v0.16.0-rc.1 \
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
