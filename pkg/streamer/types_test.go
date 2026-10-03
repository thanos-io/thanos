// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package streamer

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeDecode tests the Encode and Decode functions for StreamerRequest.
func TestEncodeDecodeStreamerRequest(t *testing.T) {
	original := StreamerRequest{
		Metric: "cpu_usage",
		LabelMatchers: []LabelMatcher{
			{Name: "instance", Value: "localhost", Type: LabelMatcher_EQ},
		},
		StartTimestampMs: 1610000000000,
		EndTimestampMs:   1610003600000,
		SkipChunks:       false,
		RequestId:        "req-123",
	}

	// Encode
	encoded, err := Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Decode
	var decoded StreamerRequest
	err = decoded.Decode(encoded)
	require.NoError(t, err)

	// Verify that decoded struct matches the original
	assert.Equal(t, original, decoded)
}

// TestEncodeDecodeStreamerResponse tests the encoding and decoding of StreamerResponse.
func TestEncodeDecodeStreamerResponse(t *testing.T) {
	original := StreamerResponse{
		Err: "test error",
		Eof: false,
		Data: &TimeSeries{
			Labels: Labels{
				{Name: "instance", Value: "localhost"},
			},
			Samples: []DataSample{
				{Timestamp: 1610000000000, Value: 3.14},
			},
		},
	}

	// Encode
	encoded, err := Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Decode
	var decoded StreamerResponse
	err = decoded.Decode(encoded)
	require.NoError(t, err)

	// Verify that decoded struct matches the original
	assert.Equal(t, original, decoded)
}

// TestDecodeInvalidBase64 tests decoding with invalid base64 input.
func TestDecodeInvalidBase64(t *testing.T) {
	invalidBase64 := "invalid_base64_string"

	var decoded StreamerRequest
	err := decoded.Decode(invalidBase64)

	assert.Error(t, err, "Expected decoding error for invalid base64 input")
}

// TestEmptyEncoding tests encoding and decoding an empty StreamerRequest.
func TestEmptyEncoding(t *testing.T) {
	original := StreamerRequest{}

	encoded, err := Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var decoded StreamerRequest
	err = decoded.Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}

// TestEncodeDecodeWithSpecialCharacters ensures that special characters in fields do not cause errors.
func TestEncodeDecodeWithSpecialCharacters(t *testing.T) {
	original := StreamerRequest{
		Metric: "cpu$usage@!%",
		LabelMatchers: []LabelMatcher{
			{Name: "inst@#$", Value: "local\nhost", Type: LabelMatcher_RE},
		},
		StartTimestampMs: 1610000000000,
		EndTimestampMs:   1610003600000,
		SkipChunks:       false,
		RequestId:        "req-456",
	}

	encoded, err := Encode(original)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var decoded StreamerRequest
	err = decoded.Decode(encoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}

// TestBase64DecodingFailure verifies that decoding fails gracefully when provided with an invalid Base64 input.
func TestBase64DecodingFailure(t *testing.T) {
	invalidBase64 := base64.StdEncoding.EncodeToString([]byte("corrupt_data"))

	var decoded StreamerResponse
	err := decoded.Decode(invalidBase64)

	assert.Error(t, err, "Expected an error while decoding corrupt data")
}

// TestDecodeWithModifiedData ensures decoding fails if base64 is tampered with.
func TestDecodeWithModifiedData(t *testing.T) {
	original := StreamerRequest{
		Metric: "memory_usage",
	}

	encoded, err := Encode(original)
	require.NoError(t, err)

	// Modify the encoded data to introduce corruption
	modifiedEncoded := encoded + "X"

	var decoded StreamerRequest
	err = decoded.Decode(modifiedEncoded)

	assert.Error(t, err, "Expected an error when decoding modified base64 data")
}

func TestDecodeWithEmptyData(t *testing.T) {
	// Empty data should return an error.
	var decoded StreamerRequest
	err := decoded.Decode("")
	assert.Error(t, err, "Expected an error when decoding empty data")
}

func TestDeliminator(t *testing.T) {
	base64Chars := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
	assert.NotContains(t, base64Chars, Deliminator)
	assert.Contains(t, base64Chars, "+")
	assert.Contains(t, base64Chars, "=")
}
