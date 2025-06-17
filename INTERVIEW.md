# Thanos Bucket Migration Tool - Requirements Interview

## Overview
New tool: `thanos bucket tools migrate` - A block migration tool to move blocks from source storage to destination storage, supporting hot-to-cold tier migration scenarios.

## Core Algorithm
1. List all blocks in source storage (with optional flag to exclude those marked for deletion)
2. Find blocks with start time older than configurable cutoff date
3. Concurrently replicate each block from hot to cold storage
4. Mark source blocks for deletion after successful replication

## Interview Responses

### Question 1: Storage Backend Support
**Q:** Are you working with object storage like S3, GCS, or Azure Blob Storage? Which specific storage backends should be supported?
**A:** Use the same object storage client as already used by Thanos bucket tools. No need to implement a new client - use the same approach as the replicate command (supports all existing Thanos storage backends).

### Question 2: Cutoff Date Configuration
**Q:** Should the cutoff date be specified as a CLI argument, config file, or both? For example, would you prefer something like `--older-than=30d` or `--cutoff-date=2024-01-01` or both options?
**A:** Support both RFC3339 absolute times and relative times. Both `--older-than=30d` and `--older-than=2025-06-01` should be valid formats.

### Question 3: Block Time Selection Criteria
**Q:** When you mention "blocks with a start time older than cutoff date" - are you referring to the block's creation timestamp, or the actual time range that the block covers (i.e., the TSDB block's min time)?
**A:** The TSDB block min time. Need to get this from the block metadata.

### Question 4: Duplicate Block Handling
**Q:** What should happen if a block already exists in the destination storage? Should the tool skip it, overwrite it, or throw an error?
**A:** This should be configurable with a flag to determine if the block is overwritten or ignored.

### Question 5: Dry-run Mode
**Q:** Should there be a dry-run mode to preview what blocks would be migrated without actually performing the migration?
**A:** Yes.

### Question 6: Deletion Timing
**Q:** Should there be a configurable delay between successful replication and marking the source block for deletion? For example, wait N hours/days to ensure the destination copy is stable before deletion?
**A:** No, immediately mark the block for deletion, but ONLY if the replication was successful.

### Question 7: Block Verification After Replication
**Q:** Should the tool verify that the destination block exists and is identical (e.g., via checksum comparison) before marking the source block for deletion?
**A:** No - this would require downloading the block again from the destination, which is not efficient. Trust that the Storage Client will do a good job of copying the data. If the data copy succeeds, consider the job done.

### Question 8: Retry Logic for Failed Migrations
**Q:** Should failed migrations be retried, and if so, how many times? Or should the tool just log the failure and continue with other blocks?
**A:** Failed migrations should be retried a configurable number of times with exponential backoff. If a block fails after retries, move on to the next block. The tool should be idempotent - successfully replicated blocks are marked for deletion and excluded from subsequent runs.

### Question 9: Logging and Tracing
**Q:** Do you need any specific logging requirements or formats? Should it integrate with existing Thanos logging patterns, or do you have specific needs for audit trails of what was migrated?
**A:** Use the same logging and tracing patterns as already used in the Thanos project.

### Question 10: Metrics and Monitoring
**Q:** Do you need metrics/monitoring integration (like Prometheus metrics) to track migration progress, success/failure rates, or other operational metrics?
**A:** No, as the job is ephemeral we rely on logs/traces for observability.

### Question 11: Size-based Filtering
**Q:** Should there be any size-based filtering options? For example, only migrate blocks larger/smaller than a certain size, or skip very small blocks that might not be worth migrating?
**A:** No, migrate everything.

### Question 12: Label/Metadata Filtering
**Q:** Should the tool support any label or metadata-based filtering beyond the date cutoff? For example, filtering by specific Prometheus labels or Thanos block metadata?
**A:** Yes, support the same `--selector.relabel-config` as present in the `thanos tools bucket web` command, to filter source blocks by external labels.

### Question 13: Concurrency Control
**Q:** Should the tool support configurable concurrency levels for the migration operations? For example, allowing users to specify how many blocks should be migrated in parallel to control resource usage and network load?
**A:** Yes definitely.

## Summary

Based on the interview, the tool requirements are:
- Use existing Thanos storage clients (all supported backends)
- Support both RFC3339 absolute and relative time formats for cutoff
- Filter blocks by TSDB block min time from metadata
- Configurable handling of existing destination blocks (overwrite/skip)
- Dry-run mode support
- Immediate deletion marking after successful replication
- No post-replication verification (trust storage client)
- Configurable retry with exponential backoff
- Idempotent operation
- Use existing Thanos logging/tracing patterns
- No metrics (ephemeral job, rely on logs/traces)
- No size-based filtering (migrate everything)
- Support `--selector.relabel-config` for label filtering
- Configurable concurrency levels

## Implementation

- **Specification**: See [SPECIFICATION.md](./SPECIFICATION.md) for the complete tool specification
- **Task Breakdown**: See [TODO.md](./TODO.md) for the detailed implementation checklist
