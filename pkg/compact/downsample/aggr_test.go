// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/efficientgo/core/testutil"
)

func TestAggrChunk(t *testing.T) {
	var input [5][]sample

	input[AggrCount] = []sample{{100, 30}, {200, 50}, {300, 60}, {400, 67}}
	input[AggrSum] = []sample{{100, 130}, {200, 1000}, {300, 2000}, {400, 5555}}
	input[AggrMin] = []sample{{100, 0}, {200, -10}, {300, 1000}, {400, -9.5}}
	// Maximum is absent.
	input[AggrCounter] = []sample{{100, 5}, {200, 10}, {300, 10.1}, {400, 15}, {400, 3}}

	var chks [5]chunkenc.Chunk

	for i, smpls := range input {
		if len(smpls) == 0 {
			continue
		}
		chks[i] = chunkenc.NewXORChunk()
		a, err := chks[i].Appender()
		testutil.Ok(t, err)

		for _, s := range smpls {
			a.Append(s.t, s.v)
		}
	}

	var res [5][]sample
	ac := EncodeAggrChunk(chks)

	for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
		if c, err := ac.Get(at); err != ErrAggrNotExist {
			testutil.Ok(t, err)
			testutil.Ok(t, expandChunkIterator(c.Iterator(nil), &res[at]))
		}
	}
	testutil.Equals(t, input, res)
}
