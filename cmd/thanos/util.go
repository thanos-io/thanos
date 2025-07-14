package main

import "github.com/prometheus/prometheus/util/compression"

// parseCompressionType parses the two compression-related configuration values and returns the CompressionType.
func parseCompressionType(compress bool, compressType compression.Type) compression.Type {
	if compress {
		return compressType
	}
	return compression.None
}
