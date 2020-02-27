// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package testutil

import "testing"

func TestTestOrBench(t *testing.T) {
	tb := NewTB(t)
	tb.Run("1", func(tb TB) { testorbenchComplexTest(tb) })
	tb.Run("2", func(tb TB) { testorbenchComplexTest(tb) })
}

func BenchmarkTestOrBench(b *testing.B) {
	tb := NewTB(b)
	tb.Run("1", func(tb TB) { testorbenchComplexTest(tb) })
	tb.Run("2", func(tb TB) { testorbenchComplexTest(tb) })
}

func testorbenchComplexTest(tb TB) {
	tb.Run("a", func(tb TB) {
		tb.Run("aa", func(tb TB) {
			tb.ResetTimer()
			for i := 0; i < tb.N(); i++ {
				if !tb.IsBenchmark() {
					if tb.N() != 1 {
						tb.FailNow()
					}
				}
			}
		})
	})
	tb.SetBytes(120220)
	tb.Run("b", func(tb TB) {
		tb.Run("bb", func(tb TB) {
			tb.ResetTimer()
			for i := 0; i < tb.N(); i++ {
				if !tb.IsBenchmark() {
					if tb.N() != 1 {
						tb.FailNow()
					}
				}
			}
		})
	})

}
