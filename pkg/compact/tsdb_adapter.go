// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
)

type tsdbCompactorAdapter struct {
	compactor *tsdb.LeveledCompactor
}

func NewTSDBCompactorAdapter(compactor *tsdb.LeveledCompactor) *tsdbCompactorAdapter {
	return &tsdbCompactorAdapter{compactor: compactor}
}

func (a tsdbCompactorAdapter) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	uid, err := a.compactor.Compact(dest, dirs, open)
	if err != nil {
		return nil, err
	}
	return []ulid.ULID{uid}, nil
}

func (a tsdbCompactorAdapter) CompactWithBlockPopulator(dest string, dirs []string, open []*tsdb.Block, blockPopulator tsdb.BlockPopulator) ([]ulid.ULID, error) {
	uid, err := a.compactor.CompactWithBlockPopulator(dest, dirs, open, blockPopulator)
	if err != nil {
		return nil, err
	}
	return []ulid.ULID{uid}, nil
}
