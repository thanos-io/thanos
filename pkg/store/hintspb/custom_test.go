// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import (
	"reflect"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestQueryStatsMerge(t *testing.T) {
	s := &QueryStats{}
	ps := reflect.Indirect(reflect.ValueOf(s))
	for i := 0; i < ps.NumField(); i++ {
		ps.FieldByIndex([]int{i}).SetInt(int64(1))
	}
	o := &QueryStats{}
	po := reflect.Indirect(reflect.ValueOf(o))
	for i := 0; i < po.NumField(); i++ {
		po.FieldByIndex([]int{i}).SetInt(int64(100))
	}
	s.Merge(o)

	// Expected stats.
	e := &QueryStats{}
	pe := reflect.Indirect(reflect.ValueOf(e))
	for i := 0; i < pe.NumField(); i++ {
		pe.FieldByIndex([]int{i}).SetInt(int64(101))
	}
	testutil.Equals(t, e, s)
}
