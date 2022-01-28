// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"sort"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block"
)

// OverlappedBlocksIssue checks bucket for blocks with overlapped time ranges.
// No repair is available for this issue.
type OverlappedBlocksIssue struct{}

func (OverlappedBlocksIssue) IssueID() string { return "overlapped_blocks" }

func (OverlappedBlocksIssue) Verify(ctx Context, idMatcher func(ulid.ULID) bool) error {
	if idMatcher != nil {
		return errors.Errorf("id matching is not supported")
	}

	level.Info(ctx.Logger).Log("msg", "started verifying issue")

	overlaps, err := fetchOverlaps(ctx, ctx.Fetcher)
	if err != nil {
		return errors.Wrap(err, "fetch overlaps")
	}

	if len(overlaps) == 0 {
		// All good.
		return nil
	}

	for k, o := range overlaps {
		level.Warn(ctx.Logger).Log("msg", "found overlapped blocks", "group", k, "overlap", o)
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
		groupKey := meta.Thanos.GroupKey()
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
