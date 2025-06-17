# Thanos Bucket Migration Tool - Specification

## Overview
New tool: `thanos bucket tools migrate` - A block migration tool to move blocks from source storage to destination storage, supporting hot-to-cold tier migration scenarios.

## Core Algorithm
1. List all blocks in source storage (with optional flag to exclude those marked for deletion)
2. Find blocks with start time older than configurable cutoff date
3. Concurrently replicate each block from hot to cold storage
4. Mark source blocks for deletion after successful replication

## Requirements Summary

Based on requirements gathering, the tool specifications are:

- **Storage Backend Support**: Use existing Thanos storage clients (all supported backends)
- **Time Configuration**: Support both RFC3339 absolute and relative time formats for cutoff
- **Block Selection**: Filter blocks by TSDB block min time from metadata
- **Duplicate Handling**: Configurable handling of existing destination blocks (overwrite/skip)
- **Preview Mode**: Dry-run mode support
- **Deletion Policy**: Immediate deletion marking after successful replication
- **Verification**: No post-replication verification (trust storage client)
- **Retry Logic**: Configurable retry with exponential backoff
- **Operation Mode**: Idempotent operation
- **Observability**: Use existing Thanos logging/tracing patterns
- **Metrics**: No metrics (ephemeral job, rely on logs/traces)
- **Size Filtering**: No size-based filtering (migrate everything)
- **Label Filtering**: Support `--selector.relabel-config` for label filtering
- **Concurrency**: Configurable concurrency levels

## CLI Interface

The tool will be invoked as:
```
thanos bucket tools migrate [flags]
```

### Required Flags
- `--objstore.config` / `--objstore.config-file`: Source storage configuration
- `--objstore-to.config` / `--objstore-to.config-file`: Destination storage configuration
- `--older-than`: Cutoff date/duration (supports RFC3339 dates and relative durations like "30d")

### Optional Flags
- `--dry-run`: Preview mode without actual migration
- `--overwrite-existing`: Overwrite blocks that already exist in destination
- `--exclude-delete-marked`: Skip blocks already marked for deletion
- `--concurrency`: Number of concurrent migration operations (default: reasonable value)
- `--max-retries`: Maximum retry attempts for failed migrations (default: 3)
- `--selector.relabel-config` / `--selector.relabel-config-file`: Filter blocks by external labels

## Operational Characteristics

- **Runtime**: Run-to-completion job suitable for CronJob execution
- **Idempotency**: Safe to run multiple times - successfully migrated blocks are marked for deletion and excluded from subsequent runs
- **Error Handling**: Individual block failures don't stop overall operation
- **Resource Usage**: Configurable concurrency to control network and storage load
- **Logging**: Comprehensive structured logging for audit trail and troubleshooting

## Use Cases

Primary use case is hot-to-cold storage tier migration:
1. Configure CronJob to run periodically (e.g., daily)
2. Set cutoff date to migrate data older than retention policy (e.g., 90 days)
3. Use label filtering to target specific tenants or data types if needed
4. Monitor logs for migration progress and any failures
