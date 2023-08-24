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

	input[AggrCount] = []sample{{t: 100, v: 30}, {t: 200, v: 50}, {t: 300, v: 60}, {t: 400, v: 67}}
	input[AggrSum] = []sample{{t: 100, v: 130}, {t: 200, v: 1000}, {t: 300, v: 2000}, {t: 400, v: 5555}}
	input[AggrMin] = []sample{{t: 100}, {t: 200, v: -10}, {t: 300, v: 1000}, {t: 400, v: -9.5}}
	// Maximum is absent.
	input[AggrCounter] = []sample{{t: 100, v: 5}, {t: 200, v: 10}, {t: 300, v: 10.1}, {t: 400, v: 15}, {t: 400, v: 3}}

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
			testutil.Ok(t, expandXorChunkIterator(c.Iterator(nil), &res[at]))
		}
	}
	testutil.Equals(t, input, res)
}
