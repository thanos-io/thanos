// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cachekey

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestParseBucketCacheKey(t *testing.T) {
	testcases := []struct {
		key         string
		expected    BucketCacheKey
		expectedErr error
	}{
		{
			key: "exists:name",
			expected: BucketCacheKey{
				Verb:  ExistsVerb,
				Name:  "name",
				Start: 0,
				End:   0,
			},
			expectedErr: nil,
		},
		{
			key: "content:name",
			expected: BucketCacheKey{
				Verb:  ContentVerb,
				Name:  "name",
				Start: 0,
				End:   0,
			},
			expectedErr: nil,
		},
		{
			key: "iter:name",
			expected: BucketCacheKey{
				Verb:  IterVerb,
				Name:  "name",
				Start: 0,
				End:   0,
			},
			expectedErr: nil,
		},
		{
			key: "attrs:name",
			expected: BucketCacheKey{
				Verb:  AttributesVerb,
				Name:  "name",
				Start: 0,
				End:   0,
			},
			expectedErr: nil,
		},
		{
			key: "subrange:name:10:20",
			expected: BucketCacheKey{
				Verb:  SubrangeVerb,
				Name:  "name",
				Start: 10,
				End:   20,
			},
			expectedErr: nil,
		},
		// Any VerbType other than SubrangeVerb should not have a "start" and "end".
		{
			key:         "iter:name:10:20",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyFormat,
		},
		// Key must always have a name.
		{
			key:         "iter",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyFormat,
		},
		// Invalid VerbType should return an error.
		{
			key:         "random:name",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyVerb,
		},
		// Start must be an integer.
		{
			key:         "subrange:name:random:10",
			expected:    BucketCacheKey{},
			expectedErr: ErrParseKeyInt,
		},
		// End must be an integer.
		{
			key:         "subrange:name:10:random",
			expected:    BucketCacheKey{},
			expectedErr: ErrParseKeyInt,
		},
		// SubrangeVerb must have start and end.
		{
			key:         "subrange:name",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyFormat,
		},
		// SubrangeVerb must have start and end both.
		{
			key:         "subrange:name:10",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyFormat,
		},
		// Key must not be an empty string.
		{
			key:         "",
			expected:    BucketCacheKey{},
			expectedErr: ErrInvalidBucketCacheKeyFormat,
		},
	}

	for _, tc := range testcases {
		res, err := ParseBucketCacheKey(tc.key)
		testutil.Equals(t, tc.expectedErr, err)
		testutil.Equals(t, tc.expected, res)
	}
}
