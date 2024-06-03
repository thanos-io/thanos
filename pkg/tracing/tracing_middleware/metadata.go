// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

package tracing_middleware

import (
	"encoding/base64"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	binHdrSuffix = "-bin"
)

// metadataTextMap extends a metadata.MD to be an opentracing textmap.
type metadataTextMap metadata.MD

// Set is a opentracing.TextMapReader interface that extracts values.
func (m metadataTextMap) Set(key, val string) {
	// gRPC allows for complex binary values to be written.
	encodedKey, encodedVal := encodeKeyValue(key, val)
	// The metadata object is a multimap, and previous values may exist, but for opentracing headers, we do not append
	// we just override.
	m[encodedKey] = []string{encodedVal}
}

// ForeachKey is a opentracing.TextMapReader interface that extracts values.
func (m metadataTextMap) ForeachKey(callback func(key, val string) error) error {
	for k, vv := range m {
		for _, v := range vv {
			if err := callback(k, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeKeyValue encodes key and value qualified for transmission via gRPC.
// note: copy pasted from private values of grpc.metadata
func encodeKeyValue(k, v string) (string, string) {
	k = strings.ToLower(k)
	if strings.HasSuffix(k, binHdrSuffix) {
		val := base64.StdEncoding.EncodeToString([]byte(v))
		v = string(val)
	}
	return k, v
}
