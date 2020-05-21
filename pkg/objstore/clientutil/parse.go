// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// ParseContentLength returns the content length (in bytes) parsed from the Content-Length
// HTTP header in input.
func ParseContentLength(m http.Header) (int64, error) {
	const name = "Content-Length"

	v, ok := m[name]
	if !ok {
		return 0, errors.Errorf("%s header not found", name)
	}

	if len(v) == 0 {
		return 0, errors.Errorf("%s header has no values", name)
	}

	ret, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "convert %s", name)
	}

	return ret, nil
}

// ParseLastModified returns the timestamp parsed from the Last-Modified
// HTTP header in input (expected to be in the RFC3339 format).
func ParseLastModified(m http.Header) (time.Time, error) {
	const name = "Last-Modified"

	v, ok := m[name]
	if !ok {
		return time.Time{}, errors.Errorf("%s header not found", name)
	}

	if len(v) == 0 {
		return time.Time{}, errors.Errorf("%s header has no values", name)
	}

	mod, err := time.Parse(time.RFC3339, v[0])
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "parse %s", name)
	}

	return mod, nil
}
