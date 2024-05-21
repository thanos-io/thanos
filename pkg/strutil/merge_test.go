// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestMergeSlices(t *testing.T) {
	var (
		a1  = []string{"Alice", "Benjamin", "Charlotte", "David", "Eleanor", "Frederick", "Grace", "Henry", "Isla", "James"}
		a2  = []string{"Ava", "Emma", "Ethan", "Isabella", "Jacob", "Liam", "Mason", "Noah", "Olivia", "Sophia"}
		a3  = []string{"Elizabeth", "George", "Katherine", "Margaret", "Michael", "Robert", "Sarah", "Thomas", "Victoria", "William"}
		a4  = []string{"Amelie", "Carlos", "Dmitri", "Elena", "Fiona", "Gustavo", "Hiro", "Ingrid", "Jorge", "Klara"}
		res = []string{"Alice", "Amelie", "Ava", "Benjamin", "Carlos", "Charlotte", "David", "Dmitri", "Eleanor", "Elena", "Elizabeth", "Emma", "Ethan", "Fiona", "Frederick", "George", "Grace", "Gustavo", "Henry", "Hiro", "Ingrid", "Isabella", "Isla", "Jacob", "James", "Jorge", "Katherine", "Klara", "Liam", "Margaret", "Mason", "Michael", "Noah", "Olivia", "Robert", "Sarah", "Sophia", "Thomas", "Victoria", "William"}
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := MergeSlices(ctx, a1, a2, a3, a4)

	testutil.Ok(t, err)
	testutil.Equals(t, result, res)
}
