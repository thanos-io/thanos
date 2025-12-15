// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import (
	"reflect"
	"testing"

	"github.com/efficientgo/core/testutil"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestQueryStatsMerge(t *testing.T) {
	s := &QueryStats{}
	ps := reflect.Indirect(reflect.ValueOf(s))
	st := ps.Type()
	for i := 0; i < ps.NumField(); i++ {
		field := ps.Field(i)
		fieldType := st.Field(i)
		// Skip unexported fields and non-int64 fields.
		if !fieldType.IsExported() || field.Kind() != reflect.Int64 {
			continue
		}
		field.SetInt(int64(1))
	}
	s.GetAllDuration = &durationpb.Duration{Seconds: 1}
	s.MergeDuration = &durationpb.Duration{Seconds: 1}

	o := &QueryStats{}
	po := reflect.Indirect(reflect.ValueOf(o))
	ot := po.Type()
	for i := 0; i < po.NumField(); i++ {
		field := po.Field(i)
		fieldType := ot.Field(i)
		// Skip unexported fields and non-int64 fields.
		if !fieldType.IsExported() || field.Kind() != reflect.Int64 {
			continue
		}
		field.SetInt(int64(100))
	}
	o.GetAllDuration = &durationpb.Duration{Seconds: 100}
	o.MergeDuration = &durationpb.Duration{Seconds: 100}

	s.Merge(o)

	// Expected stats.
	e := &QueryStats{}
	pe := reflect.Indirect(reflect.ValueOf(e))
	et := pe.Type()
	for i := 0; i < pe.NumField(); i++ {
		field := pe.Field(i)
		fieldType := et.Field(i)
		// Skip unexported fields and non-int64 fields.
		if !fieldType.IsExported() || field.Kind() != reflect.Int64 {
			continue
		}
		field.SetInt(int64(101))
	}
	e.GetAllDuration = &durationpb.Duration{Seconds: 101}
	e.MergeDuration = &durationpb.Duration{Seconds: 101}

	testutil.Equals(t, e, s)
}
