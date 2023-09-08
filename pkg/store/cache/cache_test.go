// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/crypto/blake2b"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestCacheKey_string(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)
	ulidString := uid.String()
	matcher := labels.MustNewMatcher(labels.MatchRegexp, "aaa", "bbb")
	matcher2 := labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar")

	tests := map[string]struct {
		key      cacheKey
		expected string
	}{
		"should stringify postings cache key": {
			key: cacheKey{ulidString, cacheKeyPostings(labels.Label{Name: "foo", Value: "bar"}), ""},
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"postings cache key includes compression scheme": {
			key: cacheKey{ulidString, cacheKeyPostings(labels.Label{Name: "foo", Value: "bar"}), compressionSchemeStreamedSnappy},
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s:%s", uid.String(), encodedHash, compressionSchemeStreamedSnappy)
			}(),
		},
		"should stringify series cache key": {
			key:      cacheKey{ulidString, cacheKeySeries(12345), ""},
			expected: fmt.Sprintf("S:%s:12345", uid.String()),
		},
		"should stringify expanded postings cache key": {
			key: cacheKey{ulidString, cacheKeyExpandedPostings(labelMatchersToString([]*labels.Matcher{matcher})), ""},
			expected: func() string {
				hash := blake2b.Sum256([]byte(matcher.String()))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"should stringify expanded postings cache key when multiple matchers": {
			key: cacheKey{ulidString, cacheKeyExpandedPostings(labelMatchersToString([]*labels.Matcher{matcher, matcher2})), ""},
			expected: func() string {
				hash := blake2b.Sum256([]byte(fmt.Sprintf("%s;%s", matcher.String(), matcher2.String())))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"expanded postings cache key includes compression scheme": {
			key: cacheKey{ulidString, cacheKeyExpandedPostings(labelMatchersToString([]*labels.Matcher{matcher})), compressionSchemeStreamedSnappy},
			expected: func() string {
				hash := blake2b.Sum256([]byte(matcher.String()))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s:%s", uid.String(), encodedHash, compressionSchemeStreamedSnappy)
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.key.string()
			testutil.Equals(t, testData.expected, actual)
		})
	}
}

func TestCacheKey_string_ShouldGuaranteeReasonablyShortKeyLength(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)
	ulidString := uid.String()

	tests := map[string]struct {
		keys        []cacheKey
		expectedLen int
	}{
		"should guarantee reasonably short key length for postings": {
			expectedLen: 72,
			keys: []cacheKey{
				{ulidString, cacheKeyPostings(labels.Label{Name: "a", Value: "b"}), ""},
				{ulidString, cacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}), ""},
			},
		},
		"should guarantee reasonably short key length for series": {
			expectedLen: 49,
			keys: []cacheKey{
				{ulidString, cacheKeySeries(math.MaxUint64), ""},
			},
		},
		"should guarantee reasonably short key length for expanded postings": {
			expectedLen: 73,
			keys: []cacheKey{
				{ulidString, func() interface{} {
					matchers := make([]*labels.Matcher, 0, 100)
					name := strings.Repeat("a", 100)
					value := strings.Repeat("a", 1000)
					for i := 0; i < 100; i++ {
						t := labels.MatchType(i % 4)
						matchers = append(matchers, labels.MustNewMatcher(t, name, value))
					}
					return cacheKeyExpandedPostings(labelMatchersToString(matchers))
				}(), ""},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, key := range testData.keys {
				testutil.Equals(t, testData.expectedLen, len(key.string()))
			}
		})
	}
}

func BenchmarkCacheKey_string_Postings(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := cacheKey{uid.String(), cacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}), ""}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key.string()
	}
}

func BenchmarkCacheKey_string_Series(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := cacheKey{uid.String(), cacheKeySeries(math.MaxUint64), ""}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key.string()
	}
}
