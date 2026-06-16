// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import "fmt"

// ErrInvalidConfig creates a configuration error.
func ErrInvalidConfig(msg string) error {
	return fmt.Errorf("invalid parquet config: %s", msg)
}

// ErrConversion creates a conversion error.
func ErrConversion(msg string, err error) error {
	return fmt.Errorf("parquet conversion failed: %s: %w", msg, err)
}
