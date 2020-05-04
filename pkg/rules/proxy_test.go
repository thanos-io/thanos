// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"reflect"
	"testing"

	"github.com/thanos-io/thanos/pkg/rules/rulespb"
)

func TestDedupGroups(t *testing.T) {
	for _, tc := range []struct {
		name         string
		groups, want []*rulespb.RuleGroup
	}{
		{
			name:   "no groups",
			groups: nil,
			want:   nil,
		},
		{
			name: "empty group",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
			},
		},
		{
			name: "multiple empty groups",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
		},
		{
			name: "single group",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
		},
		{
			name: "separate groups",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
			},
		},
		{
			name: "duplicate groups",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dedupGroups(tc.groups)
			if !reflect.DeepEqual(tc.want, got) {
				t.Errorf("want groups %v, got %v", tc.want, got)
			}
		})
	}
}
