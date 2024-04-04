// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import "math/rand"

// RandRange returns a random int64 from [min, max].
func RandRange(rnd *rand.Rand, min, max int64) int64 {
	return rnd.Int63n(max-min) + min
}
