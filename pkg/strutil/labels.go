// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"slices"
	"strings"
)

// ParseFlagLabels helps handle lists of labels passed from kingpin flags.
// * Split flag parts that are comma separated.
// * Remove any empty strings.
// * Sort and deduplicate the slice.
func ParseFlagLabels(f []string) []string {
	var result []string
	for _, l := range f {
		if l == "" {
			continue
		}
		result = append(result, strings.Split(l, ",")...)
	}
	slices.Sort(result)
	return slices.Compact(result)
}
