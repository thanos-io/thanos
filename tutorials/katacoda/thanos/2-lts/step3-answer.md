## Answer

**In an HA Prometheus setup with Thanos sidecars, would there be issues with multiple sidecars attempting to upload the same data blocks to object storage?**

This is handled by having unique **external labels** for all Prometheus, sidecar instances and HA replicas. To indicate that all replicas are storing same targets, they differ only in one label.

For an instance, consider the situation below:

```
First:
"cluster": "prod1"
"replica": "0"

Second:
"cluster":"prod1"
"replica": "1"
```

There is no problem with storing them since the label sets are **unique**.