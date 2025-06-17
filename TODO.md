# Thanos Bucket Migration Tool - Implementation Tasks

## Phase 1: Command Structure and Basic Setup
- [ ] **Add new migrate command to CLI structure**
  Add `migrate` subcommand to `thanos bucket tools` in `cmd/thanos/tools_bucket.go`
  Use the existing `replicate` command as a template for basic structure
  Set up command registration and basic help text

- [ ] **Create migrate command file**
  Create new file `cmd/thanos/tools_bucket_migrate.go` 
  Implement basic command structure with placeholder functionality
  Add all required CLI flags based on requirements

## Phase 2: CLI Arguments and Configuration
- [ ] **Implement cutoff date parsing**
  Add `--older-than` flag supporting both RFC3339 absolute times and relative formats (e.g., "30d", "2025-06-01")
  Create utility functions to parse and convert relative times to absolute timestamps
  Add validation for date format inputs

- [ ] **Add block handling configuration flags**
  Add `--overwrite-existing` flag to control duplicate block behavior
  Add `--dry-run` flag for preview mode
  Add `--exclude-delete-marked` flag to skip blocks already marked for deletion

- [ ] **Add operational control flags**
  Add `--concurrency` flag for parallel migration control
  Add `--max-retries` flag for configurable retry attempts
  Add `--selector.relabel-config` flag (reuse existing implementation from bucket web command)

## Phase 3: Core Migration Logic
- [ ] **Implement block discovery and filtering**
  Create function to list all blocks from source storage using existing storage client
  Implement filtering logic to exclude blocks marked for deletion (if flag set)
  Add block metadata parsing to extract TSDB min time for age comparison

- [ ] **Add cutoff date filtering**
  Implement logic to compare block min time against cutoff date
  Apply selector relabel config filtering to blocks
  Create summary logging of blocks selected for migration

- [ ] **Implement dry-run functionality**
  Add dry-run mode that lists blocks that would be migrated without performing actual migration
  Include block details (ID, size, min time) in dry-run output
  Ensure dry-run respects all filtering options

## Phase 4: Migration and Replication
- [ ] **Implement concurrent block migration**
  Create worker pool for concurrent block processing using configurable concurrency
  Reuse existing replication logic from replicate command for actual block copying
  Handle destination storage configuration and connection

- [ ] **Add duplicate block handling**
  Implement logic to check if block exists in destination
  Apply overwrite/skip behavior based on `--overwrite-existing` flag
  Log decisions for each block (skipped/overwritten)

- [ ] **Implement retry logic with exponential backoff**
  Add retry mechanism for failed migrations using configurable max retries
  Implement exponential backoff between retry attempts
  Log retry attempts and final outcomes

## Phase 5: Deletion and Cleanup
- [ ] **Implement source block deletion marking**
  Add logic to mark source blocks for deletion only after successful replication
  Ensure deletion marking only occurs when migration completes successfully
  Add appropriate logging for deletion marking activities

- [ ] **Add comprehensive error handling**
  Implement proper error handling for all migration steps
  Ensure failed migrations don't prevent processing of other blocks
  Log all errors with appropriate context and block identifiers

## Phase 6: Integration and Polish
- [ ] **Integrate logging and tracing**
  Use existing Thanos logging patterns throughout the migration process
  Add structured logging for migration progress, successes, and failures
  Integrate with existing tracing infrastructure

- [ ] **Add comprehensive testing**
  Create unit tests for date parsing utilities
  Add integration tests for the migration workflow
  Test dry-run functionality and all configuration combinations

- [ ] **Documentation and help text**
  Add comprehensive help text and examples for the migrate command
  Document all CLI flags and their usage
  Add usage examples for common scenarios (hot-to-cold migration)

## Phase 7: Validation and Finalization
- [ ] **End-to-end testing**
  Test full migration workflow with real storage backends
  Verify idempotent behavior across multiple runs
  Test error scenarios and recovery

- [ ] **Performance optimization**
  Optimize memory usage for large block lists
  Ensure efficient concurrent operations
  Add progress reporting for long-running operations

---

**Note:** Each task should be implemented as a separate commit to maintain clear development history and enable easy review of individual components.
