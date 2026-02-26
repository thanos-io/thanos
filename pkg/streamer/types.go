// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package streamer

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
)

type LabelMatcher_Type int32

const (
	// Must be the same with the values in github.com/thanos-io/thanos@v0.30.0/pkg/store/storepb/types.pb.go .
	LabelMatcher_EQ  LabelMatcher_Type = 0
	LabelMatcher_NEQ LabelMatcher_Type = 1
	LabelMatcher_RE  LabelMatcher_Type = 2
	LabelMatcher_NRE LabelMatcher_Type = 3
)

var (
	// NB: don't change. The socket reader uses a scanner to read a line ended by a newline.
	Deliminator = "\n"
)

type LabelMatcher struct {
	// The label name to match.
	Name string
	// The regular expression to match against the label value.
	Value string
	Type  LabelMatcher_Type
}

type StreamerRequest struct {
	// Can be empty to get all metrics.
	Metric        string
	LabelMatchers []LabelMatcher
	// In milliseconds since epoch
	StartTimestampMs int64
	// In milliseconds since epoch
	EndTimestampMs int64
	SkipChunks     bool
	RequestId      string
}

type Label struct {
	Name  string
	Value string
}

type Labels []Label

type DataSample struct {
	// In milliseconds since epoch
	Timestamp int64
	Value     float64
}

type TimeSeries struct {
	// Labels are sorted by their names.
	Labels  Labels
	Samples []DataSample
}

type StreamerResponse struct {
	// If the response is an error, this field will be non-empty.
	Err string
	// True if no more data will be sent and this response has no data either.
	Eof  bool
	Data *TimeSeries
}

func Encode(e any) (string, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(e)
	if err != nil {
		return "", err
	}

	binaryData := buf.Bytes()
	base64Encoded := base64.StdEncoding.EncodeToString(binaryData)
	return base64Encoded, nil
}

func decode(e any, base64Encoded string) error {
	base64Decoded, err := base64.StdEncoding.DecodeString(base64Encoded)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(base64Decoded)
	decoder := gob.NewDecoder(buf)
	err = decoder.Decode(e)
	return err
}

func (s *StreamerRequest) Decode(base64Encoded string) error {
	return decode(s, base64Encoded)
}

func (s *StreamerResponse) Decode(base64Encoded string) error {
	return decode(s, base64Encoded)
}
