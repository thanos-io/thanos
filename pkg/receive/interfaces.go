// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

// fileContent is an interface to avoid a direct dependency on kingpin or extkingpin.
type fileContent interface {
	Content() ([]byte, error)
	Path() string
}
