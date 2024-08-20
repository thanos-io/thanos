// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/extpromql"
)

func Test_parseFlagLabels(t *testing.T) {
	var tData = []struct {
		s         []string
		expectErr bool
	}{
		{
			s:         []string{`labelName="LabelVal"`, `_label_Name="LabelVal"`, `label_name="LabelVal"`, `LAb_el_Name="LabelValue"`, `lab3l_Nam3="LabelValue"`},
			expectErr: false,
		},
		{
			s:         []string{`label-Name="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`label:Name="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`1abelName="LabelVal"`}, // Unsupported labelname.
			expectErr: true,
		},
		{
			s:         []string{`label_Name"LabelVal"`}, // Missing "=" separator.
			expectErr: true,
		},
		{
			s:         []string{`label_Name= "LabelVal"`}, // Whitespace invalid syntax.
			expectErr: true,
		},
		{
			s:         []string{`label_name=LabelVal`}, // Missing quotes invalid syntax.
			expectErr: true,
		},
	}
	for _, td := range tData {
		_, err := parseFlagLabels(td.s)
		testutil.Equals(t, err != nil, td.expectErr)
	}
}

func Test_validateTemplate(t *testing.T) {
	tData := []struct {
		template  string
		expectErr bool
	}{
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expression}}&g0.tab=1`,
			expectErr: true,
		},
		{
			template:  `another template includes {{.Expr}}`,
			expectErr: false,
		},
	}
	for _, td := range tData {
		err := validateTemplate(td.template)
		testutil.Equals(t, err != nil, td.expectErr)
	}
}

func Test_tableLinkForExpression(t *testing.T) {
	tData := []struct {
		template  string
		expr      string
		expectStr string
		expectErr bool
	}{
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expr:      `up{app="foo"}`,
			expectStr: `/graph?g0.expr=up%7Bapp%3D%22foo%22%7D&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expr:      `up{app="foo yoo"}`,
			expectStr: `/graph?g0.expr=up%7Bapp%3D%22foo+yoo%22%7D&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expression}}&g0.tab=1`,
			expr:      "test_expr",
			expectErr: true,
		},
		{
			template:  `another template includes {{.Expr}}`,
			expr:      "test_expr",
			expectStr: `another template includes test_expr`,
			expectErr: false,
		},
	}
	for _, td := range tData {
		resStr, err := tableLinkForExpression(td.template, td.expr)
		testutil.Equals(t, err != nil, td.expectErr)
		testutil.Equals(t, resStr, td.expectStr)
	}
}

func TestFilterOutPromQLWarnings(t *testing.T) {
	logger := log.NewNopLogger()
	query := "foo"
	expr, err := extpromql.ParseExpr(`rate(prometheus_build_info[5m])`)
	testutil.Ok(t, err)
	possibleCounterInfo := annotations.NewPossibleNonCounterInfo("foo", expr.PositionRange())
	badBucketLabelWarning := annotations.NewBadBucketLabelWarning("foo", "0.99", expr.PositionRange())
	for _, tc := range []struct {
		name     string
		warnings []string
		expected []string
	}{
		{
			name:     "nil warning",
			expected: make([]string, 0),
		},
		{
			name:     "empty warning",
			warnings: make([]string, 0),
			expected: make([]string, 0),
		},
		{
			name: "no PromQL warning",
			warnings: []string{
				"some_warning_message",
			},
			expected: []string{
				"some_warning_message",
			},
		},
		{
			name: "PromQL warning",
			warnings: []string{
				possibleCounterInfo.Error(),
			},
			expected: make([]string, 0),
		},
		{
			name: "filter out all PromQL warnings",
			warnings: []string{
				possibleCounterInfo.Error(),
				badBucketLabelWarning.Error(),
				"some_warning_message",
			},
			expected: []string{
				"some_warning_message",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := filterOutPromQLWarnings(tc.warnings, logger, query)
			testutil.Equals(t, tc.expected, output)
		})
	}
}
