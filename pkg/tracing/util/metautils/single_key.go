// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

package metautils

import (
	"encoding/base64"
	"strings"
)

const (
	binHdrSuffix = "-bin"
)

func encodeKeyValue(k, v string) (string, string) {
	k = strings.ToLower(k)
	if strings.HasSuffix(k, binHdrSuffix) {
		return k, base64.StdEncoding.EncodeToString([]byte(v))
	}
	return k, v
}
