// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package wlogutil

import "github.com/prometheus/prometheus/util/compression"

// ParseCompressionType parses the two compression-related configuration values and returns the CompressionType.
func ParseCompressionType(compress bool, compressType compression.Type) compression.Type {
	if compress {
		return compressType
	}
	return compression.None
}
