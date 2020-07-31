### What about long term storage?

This is as easy as small change to prometheus.yaml:

`manifests/prometheus/prometheus.yaml`{{open}}

<pre class="file" data-filename="/root/manifests/prometheus/prometheus.yaml" data-target="insert"  data-marker="    # Thanos sidecar will be now included!">    objectStorageConfig:
      key: thanos.yaml
      name: thanos-objstore-config</pre>

And defining `thanos-objstore-config` secret with `thanos.yaml` file:

```yaml
type: s3
config:
  bucket: thanos
  endpoint: <objectstorage.endpoint.url>
  access_key: XXX
  secret_key: XXX
```
 
On the way you might want to make the retention much shorter and disable local Prometheus compaction with `disableCompaction` custom resource option.

With that you can check out other tutorials how to read blocks from object storage using [Store Gateway](https://thanos.io/components/store.md/) (: 