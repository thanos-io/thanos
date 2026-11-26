// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/crypto/blake2b"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
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
		key      CacheKey
		expected string
	}{
		"should stringify postings cache key": {
			key: CacheKey{ulidString, CacheKeyPostings(labels.Label{Name: "foo", Value: "bar"}), ""},
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"postings cache key includes compression scheme": {
			key: CacheKey{ulidString, CacheKeyPostings(labels.Label{Name: "foo", Value: "bar"}), compressionSchemeStreamedSnappy},
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s:%s", uid.String(), encodedHash, compressionSchemeStreamedSnappy)
			}(),
		},
		"should stringify series cache key": {
			key:      CacheKey{ulidString, CacheKeySeries(12345), ""},
			expected: fmt.Sprintf("S:%s:12345", uid.String()),
		},
		"should stringify expanded postings cache key": {
			key: CacheKey{ulidString, CacheKeyExpandedPostings(LabelMatchersToString([]*labels.Matcher{matcher})), ""},
			expected: func() string {
				hash := blake2b.Sum256([]byte(matcher.String()))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"should stringify expanded postings cache key when multiple matchers": {
			key: CacheKey{ulidString, CacheKeyExpandedPostings(LabelMatchersToString([]*labels.Matcher{matcher, matcher2})), ""},
			expected: func() string {
				hash := blake2b.Sum256(fmt.Appendf(nil, "%s;%s", matcher.String(), matcher2.String()))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"expanded postings cache key includes compression scheme": {
			key: CacheKey{ulidString, CacheKeyExpandedPostings(LabelMatchersToString([]*labels.Matcher{matcher})), compressionSchemeStreamedSnappy},
			expected: func() string {
				hash := blake2b.Sum256([]byte(matcher.String()))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("EP:%s:%s:%s", uid.String(), encodedHash, compressionSchemeStreamedSnappy)
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.key.String()
			testutil.Equals(t, testData.expected, actual)
		})
	}
}

func TestCacheKey_string_ShouldGuaranteeReasonablyShortKeyLength(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)
	ulidString := uid.String()

	tests := map[string]struct {
		keys        []CacheKey
		expectedLen int
	}{
		"should guarantee reasonably short key length for postings": {
			expectedLen: 72,
			keys: []CacheKey{
				{ulidString, CacheKeyPostings(labels.Label{Name: "a", Value: "b"}), ""},
				{ulidString, CacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}), ""},
			},
		},
		"should guarantee reasonably short key length for series": {
			expectedLen: 49,
			keys: []CacheKey{
				{ulidString, CacheKeySeries(math.MaxUint64), ""},
			},
		},
		"should guarantee reasonably short key length for expanded postings": {
			expectedLen: 73,
			keys: []CacheKey{
				{ulidString, func() any {
					matchers := make([]*labels.Matcher, 0, 100)
					name := strings.Repeat("a", 100)
					value := strings.Repeat("a", 1000)
					for i := range 100 {
						t := labels.MatchType(i % 4)
						matchers = append(matchers, labels.MustNewMatcher(t, name, value))
					}
					return CacheKeyExpandedPostings(LabelMatchersToString(matchers))
				}(), ""},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, key := range testData.keys {
				testutil.Equals(t, testData.expectedLen, len(key.String()))
			}
		})
	}
}

func TestCacheTTL(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		meta        *metadata.Meta
		expectedTTL time.Duration
	}{
		"30m": {
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					MinTime: 1730000000000,
					MaxTime: 1730001800000,
				},
			},
			expectedTTL: time.Hour,
		},
		"1h": {
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					MinTime: 1730000000000,
					MaxTime: 1730003600000,
				},
			},
			expectedTTL: time.Hour,
		},
		"1h 1m": {
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					MinTime: 1730000000000,
					MaxTime: 1730003660000,
				},
			},
			expectedTTL: time.Hour * 2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			testutil.Equals(t, testData.expectedTTL, CacheTTL(testData.meta))
		})
	}
}

func BenchmarkCacheKey_string_Postings(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := CacheKey{uid.String(), CacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}), ""}

	for b.Loop() {
		_ = key.String()
	}
}

func BenchmarkCacheKey_string_Series(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := CacheKey{uid.String(), CacheKeySeries(math.MaxUint64), ""}

	for b.Loop() {
		_ = key.String()
	}
}
