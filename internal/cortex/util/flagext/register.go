// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package flagext

import "flag"

// Registerer is a thing that can RegisterFlags
type Registerer interface {
	RegisterFlags(*flag.FlagSet)
}

// DefaultValues initiates a set of configs (Registerers) with their defaults.
func DefaultValues(rs ...Registerer) {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	for _, r := range rs {
		r.RegisterFlags(fs)
	}
	_ = fs.Parse([]string{})
}
