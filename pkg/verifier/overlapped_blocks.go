// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
)

const OverlappedBlocksIssueID = "overlapped_blocks"

// OverlappedBlocksIssue checks bucket for blocks with overlapped time ranges.
// No repair is available for this issue.
func OverlappedBlocksIssue(ctx context.Context, logger log.Logger, _ objstore.Bucket, _ objstore.Bucket, repair bool, idMatcher func(ulid.ULID) bool, fetcher block.MetadataFetcher, _ time.Duration, _ *verifierMetrics) error {
	if idMatcher != nil {
		return errors.Errorf("id matching is not supported by issue %s verifier", OverlappedBlocksIssueID)
	}

	level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", OverlappedBlocksIssueID)

	overlaps, err := fetchOverlaps(ctx, fetcher)
	if err != nil {
		return errors.Wrap(err, OverlappedBlocksIssueID)
	}

	if len(overlaps) == 0 {
		// All good.
		return nil
	}

	for k, o := range overlaps {
		level.Warn(logger).Log("msg", "found overlapped blocks", "group", k, "overlap", o)
	}

	if repair {
		level.Warn(logger).Log("msg", "repair is not implemented for this issue", "issue", OverlappedBlocksIssueID)
	}
	return nil
}

func fetchOverlaps(ctx context.Context, fetcher block.MetadataFetcher) (map[string]tsdb.Overlaps, error) {
	metas, _, err := fetcher.Fetch(ctx)
	if err != nil {
		return nil, err
	}

	groupMetasMap := map[string][]tsdb.BlockMeta{}
	for _, meta := range metas {
		groupKey := compact.DefaultGroupKey(meta.Thanos)
		groupMetasMap[groupKey] = append(groupMetasMap[groupKey], meta.BlockMeta)
	}

	overlaps := map[string]tsdb.Overlaps{}
	for k, groupMetas := range groupMetasMap {

		sort.Slice(groupMetas, func(i, j int) bool {
			return groupMetas[i].MinTime < groupMetas[j].MinTime
		})

		o := tsdb.OverlappingBlocks(groupMetas)
		if len(o) > 0 {
			overlaps[k] = o
		}
	}

	return overlaps, nil
}
