# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## Unreleased

- Add Thanos Rule UI
- Add Thanos Rule reload via HTTP POST to /-/reload
- Add Thanos compact --retention.default flag, for configuring storage bucket retention period.
- Removes support for multiple units in duration. For example: 1m0s won't work, while 1m will work.
- Adds support for y,w,d time units 
- Add Thanos bucket ls -o wide, which provides more detailed information about blocks stored in the bucket.

Newest release candidate: [v0.1.0-rc.2](https://github.com/improbable-eng/thanos/releases/tag/v0.1.0-rc.2)

### Added
- Gossip layer for all components.
- StoreAPI gRPC proto.
- TSDB block upload logic for Sidecar.
- StoreAPI logic for Sidecar.
- Config and rule reloader logic for Sidecar.
- On-the fly result merge and deduplication logic for Querier.
- Custom Thanos UI (based mainly on Prometheus UI) for Querier.
- StoreAPI logic for Store.
- Optimized object storage fetch logic for Store.
- Index cache and chunk pool for Store for better memory usage.
- Stable support for Google Cloud Storage object storage.
- StoreAPI logic for Querier to support Thanos federation (experimental).
- Support for S3 minio-based AWS object storage (experimental).
- Compaction logic of blocks from multiple sources for Compactor.
- Downsampling logic for Compactor (experimental).
- Rule (including alerts) evaluation logic for Ruler (experimental).
- StoreAPI logic for Ruler.
- Basic metric orchestration for all components.
- Verify commands with potential fixes (experimental).
- Compact / Downsample offline commands.
- Bucket commands.
- Downsampling support for UI.
- Grafana dashboards for Thanos components.

