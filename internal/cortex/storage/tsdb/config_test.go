// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tsdb

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thanos-io/thanos/internal/cortex/storage/bucket"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup       func(*BlocksStorageConfig)
		expectedErr error
	}{
		"should pass on S3 backend": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.Bucket.Backend = "s3"
			},
			expectedErr: nil,
		},
		"should pass on GCS backend": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.Bucket.Backend = "gcs"
			},
			expectedErr: nil,
		},
		"should fail on unknown storage backend": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.Bucket.Backend = "unknown"
			},
			expectedErr: bucket.ErrUnsupportedStorageBackend,
		},
		"should fail on invalid ship concurrency": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.ShipConcurrency = 0
			},
			expectedErr: errInvalidShipConcurrency,
		},
		"should pass on invalid ship concurrency but shipping is disabled": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.ShipConcurrency = 0
				cfg.TSDB.ShipInterval = 0
			},
			expectedErr: nil,
		},
		"should fail on invalid opening concurrency": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.MaxTSDBOpeningConcurrencyOnStartup = 0
			},
			expectedErr: errInvalidOpeningConcurrency,
		},
		"should fail on invalid compaction interval": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.HeadCompactionInterval = 0
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on too high compaction interval": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.HeadCompactionInterval = 10 * time.Minute
			},
			expectedErr: errInvalidCompactionInterval,
		},
		"should fail on invalid compaction concurrency": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.HeadCompactionConcurrency = 0
			},
			expectedErr: errInvalidCompactionConcurrency,
		},
		"should pass on valid compaction concurrency": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.HeadCompactionConcurrency = 10
			},
			expectedErr: nil,
		},
		"should fail on negative stripe size": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.StripeSize = -2
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 0": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.StripeSize = 0
			},
			expectedErr: errInvalidStripeSize,
		},
		"should fail on stripe size 1": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.StripeSize = 1
			},
			expectedErr: errInvalidStripeSize,
		},
		"should pass on valid stripe size": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.StripeSize = 1 << 14
			},
			expectedErr: nil,
		},
		"should fail on empty block ranges": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.BlockRanges = nil
			},
			expectedErr: errEmptyBlockranges,
		},
		"should fail on invalid TSDB WAL segment size": {
			setup: func(cfg *BlocksStorageConfig) {
				cfg.TSDB.WALSegmentSizeBytes = 0
			},
			expectedErr: errInvalidWALSegmentSizeBytes,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			cfg := &BlocksStorageConfig{}
			flagext.DefaultValues(cfg)
			testData.setup(cfg)

			actualErr := cfg.Validate()
			assert.Equal(t, testData.expectedErr, actualErr)
		})
	}
}

func TestConfig_DurationList(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg            BlocksStorageConfig
		expectedRanges []int64
		f              func(*BlocksStorageConfig)
	}{
		"default to 2h": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
		"parse ranges correctly": {
			cfg: BlocksStorageConfig{
				TSDB: TSDBConfig{
					BlockRanges: []time.Duration{
						2 * time.Hour,
						10 * time.Hour,
						50 * time.Hour,
					},
				},
			},
			expectedRanges: []int64{7200000, 36000000, 180000000},
			f:              func(*BlocksStorageConfig) {},
		},
		"handle multiple flag parse": {
			cfg:            BlocksStorageConfig{},
			expectedRanges: []int64{7200000},
			f: func(c *BlocksStorageConfig) {
				c.RegisterFlags(&flag.FlagSet{})
				c.RegisterFlags(&flag.FlagSet{})
			},
		},
	}

	for name, data := range tests {
		testdata := data

		t.Run(name, func(t *testing.T) {
			testdata.f(&testdata.cfg)
			assert.Equal(t, testdata.expectedRanges, testdata.cfg.TSDB.BlockRanges.ToMilliseconds())
		})
	}
}
