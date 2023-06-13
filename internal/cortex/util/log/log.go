// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package log

import (
	kitlog "github.com/go-kit/log"
)

var (
	// Logger is a shared go-kit logger.
	// TODO: Change all components to take a non-global logger via their constructors.
	// Prefer accepting a non-global logger as an argument.
	Logger = kitlog.NewNopLogger()
)
