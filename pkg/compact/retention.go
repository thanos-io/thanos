// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	// tenantRetentionRegex is the regex pattern for parsing tenant retention.
	// valid format is `<tenant>:(<yyyy-mm-dd>|<duration>d)(:all)?` where <duration> > 0.
	// Default behavior is to delete only level 1 blocks, use :all to delete all blocks.
	// Use `*` as tenant name to apply policy to all tenants (as a default/fallback).
	// Specific tenant policies take precedence over the wildcard policy.
	tenantRetentionRegex = `^([\w-]+|\*):((\d{4}-\d{2}-\d{2})|(\d+d))(:all)?$`
	wildCardTenant       = "*"

	Level1 = 1 // compaction level 1 indicating a new block
	Level2 = 2 // compaction level 2 indicating a compacted block
)

// ApplyRetentionPolicyByResolution removes blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func ApplyRetentionPolicyByResolution(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	retentionByResolution map[ResolutionLevel]time.Duration,
	blocksMarkedForDeletion prometheus.Counter,
) error {
	level.Info(logger).Log("msg", "start optional retention")
	wg := &sync.WaitGroup{}
	sem := make(chan struct{}, runtime.NumCPU())
	for id, m := range metas {
		retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
		if retentionDuration.Seconds() == 0 {
			continue
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(retentionDuration)) {
			level.Info(logger).Log("msg", "applying retention: marking block for deletion", "id", id, "maxTime", maxTime.String())
			sem <- struct{}{} // acquire BEFORE spawning goroutine
			wg.Add(1)
			go func(wg *sync.WaitGroup, sem chan struct{}, id ulid.ULID, retentionDuration time.Duration) {
				defer wg.Done()
				defer func() { <-sem }() // release
				if err := block.MarkForDeletion(ctx, logger, bkt, id, fmt.Sprintf("block exceeding retention of %v", retentionDuration), blocksMarkedForDeletion); err != nil {
					level.Error(logger).Log("msg", "failed to mark block for deletion", "id", id, "err", err)
				}
			}(wg, sem, id, retentionDuration)
		}
	}
	wg.Wait()
	level.Info(logger).Log("msg", "optional retention apply done")
	return nil
}

type RetentionPolicy struct {
	CutoffDate        time.Time
	RetentionDuration time.Duration
	IsAll             bool // IsAll indicates if the retention policy applies to all blocks. Default is false (level 1 only).
}

func (r RetentionPolicy) isExpired(blockMaxTime time.Time) bool {
	if r.CutoffDate.IsZero() {
		return time.Now().After(blockMaxTime.Add(r.RetentionDuration))
	}
	return r.CutoffDate.After(blockMaxTime)
}

func ParesRetentionPolicyByTenant(logger log.Logger, retentionTenants []string) (map[string]RetentionPolicy, error) {
	pattern := regexp.MustCompile(tenantRetentionRegex)
	retentionByTenant := make(map[string]RetentionPolicy, len(retentionTenants))
	for _, tenantRetention := range retentionTenants {
		matches := pattern.FindStringSubmatch(tenantRetention)
		invalidFormat := errors.Errorf("invalid retention format for tenant: %s, must be `<tenant>:(<yyyy-mm-dd>|<duration>d)(:all)?`", tenantRetention)
		if matches == nil {
			return nil, errors.Wrapf(invalidFormat, "matched size %d", len(matches))
		}
		tenant := matches[1]
		var policy RetentionPolicy
		if _, ok := retentionByTenant[tenant]; ok {
			return nil, errors.Errorf("duplicate retention policy for tenant: %s", tenant)
		}
		if cutoffDate, err := time.Parse(time.DateOnly, matches[3]); matches[3] != "" {
			if err != nil {
				return nil, errors.Wrapf(invalidFormat, "error parsing cutoff date: %v", err)
			}
			policy.CutoffDate = cutoffDate
		}
		if duration, err := model.ParseDuration(matches[4]); matches[4] != "" {
			if err != nil {
				return nil, errors.Wrapf(invalidFormat, "error parsing duration: %v", err)
			} else if duration == 0 {
				return nil, errors.Wrapf(invalidFormat, "duration must be greater than 0")
			}
			policy.RetentionDuration = time.Duration(duration)
		}
		policy.IsAll = len(matches) > 5 && matches[5] == ":all"
		level.Info(logger).Log("msg", "retention policy for tenant is enabled", "tenant", tenant, "retention policy", fmt.Sprintf("%v", policy))
		retentionByTenant[tenant] = policy
	}
	return retentionByTenant, nil
}

// ApplyRetentionPolicyByTenant removes blocks depending on the specified retentionByTenant based on blocks MaxTime.
// The wildcard policy ("*") applies to all tenants as a default/fallback.
// Specific tenant policies take precedence over the wildcard policy.
func ApplyRetentionPolicyByTenant(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	retentionByTenant map[string]RetentionPolicy,
	blocksMarkedForDeletion prometheus.Counter) error {
	if len(retentionByTenant) == 0 {
		level.Info(logger).Log("msg", "tenant retention is disabled due to no policy")
		return nil
	}
	level.Info(logger).Log("msg", "start tenant retention", "total", len(metas))
	deleted, skipped, notExpired := 0, 0, 0
	// Check if wildcard policy exists
	wildcardPolicy, hasWildcard := retentionByTenant[wildCardTenant]
	for id, m := range metas {
		tenant := m.Thanos.GetTenant()
		// First try to find tenant-specific policy
		policy, ok := retentionByTenant[tenant]
		if !ok {
			// Fallback to wildcard policy if tenant-specific policy not found
			if hasWildcard {
				policy = wildcardPolicy
			} else {
				skipped++
				continue
			}
		}
		maxTime := time.Unix(m.MaxTime/1000, 0)
		// Default behavior: only delete level 1 blocks unless IsAll is true
		if !policy.IsAll && m.Compaction.Level != Level1 {
			continue
		}
		if policy.isExpired(maxTime) {
			level.Info(logger).Log("msg", "deleting blocks applying retention policy", "id", id, "tenant", tenant, "maxTime", maxTime.String())
			if err := block.Delete(ctx, logger, bkt, id); err != nil {
				level.Error(logger).Log("msg", "failed to delete block", "id", id, "err", err)
				continue // continue to next block to clean up backlogs
			} else {
				blocksMarkedForDeletion.Inc()
				deleted++
			}
		} else {
			notExpired++
		}
	}
	level.Info(logger).Log("msg", "tenant retention apply done", "deleted", deleted, "skipped", skipped, "notExpired", notExpired)
	return nil
}

// ApplyFastRetentionByTenant applies tenant-based retention policies using streaming deletion.
// Unlike ApplyRetentionPolicyByTenant, this processes blocks one-by-one without syncing all metadata first.
// If cacheDir is set and contains block directories, it iterates from cache (enabling resume).
// Otherwise, it lists blocks from the bucket.
// Blocks without tenant label are deleted immediately as corrupt.
func ApplyFastRetentionByTenant(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	retentionByTenant map[string]RetentionPolicy,
	cacheDir string,
	concurrency int,
	dryRun bool,
	blocksDeleted prometheus.Counter,
) error {
	if len(retentionByTenant) == 0 {
		level.Info(logger).Log("msg", "fast retention is disabled due to no policy")
		return nil
	}

	startTime := time.Now()
	var scanned, deleted, corrupt, skipped, errCount atomic.Int64

	blockChan := make(chan ulid.ULID, concurrency*2)
	eg, gCtx := errgroup.WithContext(ctx)

	// Check if wildcard policy exists
	wildcardPolicy, hasWildcard := retentionByTenant[wildCardTenant]

	// Start producer goroutine
	eg.Go(func() error {
		defer close(blockChan)
		return produceBlockIDs(gCtx, logger, bkt, cacheDir, blockChan)
	})

	// Start consumer goroutines
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for id := range blockChan {
				scanned.Inc()

				// Load meta from cache or bucket
				meta, err := loadMetaFromCacheOrBucket(gCtx, logger, bkt, id, cacheDir)
				if err != nil {
					// Any block without meta.json is corrupt/incomplete and should be deleted
					// meta.json is the first object created, so its absence indicates corruption
					level.Warn(logger).Log("msg", "deleting corrupt block (missing meta.json)", "id", id, "err", err)
					if !dryRun {
						if err := deleteBlockAndCache(gCtx, logger, bkt, id, cacheDir); err != nil {
							level.Error(logger).Log("msg", "failed to delete corrupt block", "id", id, "err", err)
							errCount.Inc()
							continue
						}
					}
					blocksDeleted.Inc()
					corrupt.Inc()
					continue
				}

				tenant := meta.Thanos.GetTenant()

				// Blocks without tenant label are corrupt - delete immediately
				if tenant == metadata.DefaultTenant {
					level.Warn(logger).Log("msg", "deleting corrupt block (no tenant label)", "id", id)
					if !dryRun {
						if err := deleteBlockAndCache(gCtx, logger, bkt, id, cacheDir); err != nil {
							level.Error(logger).Log("msg", "failed to delete corrupt block", "id", id, "err", err)
							errCount.Inc()
							continue
						}
					}
					blocksDeleted.Inc()
					corrupt.Inc()
					continue
				}

				// Look up retention policy (tenant-specific first, then wildcard)
				policy, ok := retentionByTenant[tenant]
				if !ok {
					if hasWildcard {
						policy = wildcardPolicy
					} else {
						skipped.Inc()
						continue
					}
				}

				// Default behavior: only delete level 1 blocks unless IsAll is true
				if !policy.IsAll && meta.Compaction.Level != Level1 {
					skipped.Inc()
					continue
				}

				maxTime := time.Unix(meta.MaxTime/1000, 0)
				if policy.isExpired(maxTime) {
					level.Info(logger).Log("msg", "deleting expired block", "id", id, "tenant", tenant, "maxTime", maxTime.String())
					if !dryRun {
						if err := deleteBlockAndCache(gCtx, logger, bkt, id, cacheDir); err != nil {
							level.Error(logger).Log("msg", "failed to delete block", "id", id, "err", err)
							errCount.Inc()
							continue
						}
					}
					blocksDeleted.Inc()
					deleted.Inc()
				} else {
					skipped.Inc()
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "fast retention processing")
	}

	level.Info(logger).Log(
		"msg", "fast retention completed",
		"scanned", scanned.Load(),
		"deleted", deleted.Load(),
		"corrupt", corrupt.Load(),
		"skipped", skipped.Load(),
		"errors", errCount.Load(),
		"duration", time.Since(startTime),
		"dry_run", dryRun,
	)
	return nil
}

// produceBlockIDs sends block IDs to the channel, reading from cache or bucket.
func produceBlockIDs(ctx context.Context, logger log.Logger, bkt objstore.Bucket, cacheDir string, blockChan chan<- ulid.ULID) error {
	level.Info(logger).Log("msg", "starting block ID production", cacheDir, cacheDir)
	// If cache dir is set and has block directories, iterate from cache
	if cacheDir != "" {
		entries, err := os.ReadDir(cacheDir)
		if err == nil && len(entries) > 0 {
			level.Info(logger).Log("msg", "iterating blocks from local cache", "cache_dir", cacheDir, "entries", len(entries))
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				id, ok := block.IsBlockDir(entry.Name())
				if !ok {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case blockChan <- id:
				}
			}
			return nil
		}
		if err != nil && !os.IsNotExist(err) {
			level.Warn(logger).Log("msg", "failed to read cache dir, falling back to bucket", "err", err)
		}
	}

	// Fallback: iterate from bucket
	level.Info(logger).Log("msg", "iterating blocks from bucket")
	return bkt.Iter(ctx, "", func(name string) error {
		// Only process top-level directories (block ULIDs)
		if strings.Count(name, "/") > 1 {
			return nil
		}
		name = strings.TrimSuffix(name, "/")
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockChan <- id:
		}
		return nil
	})
}

// loadMetaFromCacheOrBucket loads meta.json from local cache first, then falls back to bucket.
func loadMetaFromCacheOrBucket(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID, cacheDir string) (*metadata.Meta, error) {
	// Try cache first
	if cacheDir != "" {
		cachedBlockDir := filepath.Join(cacheDir, id.String())
		m, err := metadata.ReadFromDir(cachedBlockDir)
		if err == nil {
			return m, nil
		}
		if !os.IsNotExist(err) {
			level.Debug(logger).Log("msg", "failed to read meta from cache", "id", id, "err", err)
		}
	}

	// Fallback to bucket
	m, err := block.DownloadMeta(ctx, logger, bkt, id)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// deleteBlockAndCache deletes a block from bucket and removes from local cache.
func deleteBlockAndCache(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID, cacheDir string) error {
	// Delete from bucket first
	if err := block.Delete(ctx, logger, bkt, id); err != nil {
		return err
	}

	// Delete from local cache (best effort)
	if cacheDir != "" {
		cachedBlockDir := filepath.Join(cacheDir, id.String())
		if err := os.RemoveAll(cachedBlockDir); err != nil && !os.IsNotExist(err) {
			level.Warn(logger).Log("msg", "failed to remove block from cache", "id", id, "err", err)
		}
	}

	return nil
}
