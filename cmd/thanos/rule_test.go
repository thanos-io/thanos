// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
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
			s:         []string{`label_Name"LabelVal"`}, // Missing "=" seprator.
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
