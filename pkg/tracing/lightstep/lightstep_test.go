// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package lightstep

import (
	"os"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"

	"github.com/opentracing/opentracing-go"
)

func TestParseTags(t *testing.T) {
	type testData struct {
		input          string
		description    string
		expectedOutput opentracing.Tags
	}

	testingData := []testData{
		{
			description:    `A very simple case, key "foo" and value "bar"`,
			input:          "foo=bar",
			expectedOutput: opentracing.Tags{"foo": "bar"},
		},
		{
			description:    `A simple case multiple keys, keys ["foo", "bar"] and values ["foo", "bar"]`,
			input:          "foo=foo,bar=bar",
			expectedOutput: opentracing.Tags{"foo": "foo", "bar": "bar"},
		},
		{
			description:    `A case with empty environment variable, key "foo" and value ""`,
			input:          "foo=${TEST:}",
			expectedOutput: opentracing.Tags{"foo": ""},
		},
		{
			description:    `A case with empty environment variable, key "foo" and value ""`,
			input:          "foo=${TEST:}",
			expectedOutput: opentracing.Tags{"foo": ""},
		},
		{
			description:    `A case with default environment variable, key "foo" and value "default"`,
			input:          "foo=${TEST:default}",
			expectedOutput: opentracing.Tags{"foo": "default"},
		},
		{
			description:    `A case with real environment variable, key "foo" and value "env-bar"`,
			input:          "foo=${_TEST_PARSE_TAGS:default}",
			expectedOutput: opentracing.Tags{"foo": "env-bar"},
		},
	}

	os.Setenv("_TEST_PARSE_TAGS", "env-bar")
	for _, test := range testingData {
		t.Logf("testing %s\n", test.description)
		testutil.Equals(t, test.expectedOutput, parseTags(test.input))
	}
}
