package storecache

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestCacheKey_string(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)

	tests := map[string]struct {
		key      cacheKey
		expected string
	}{
		"should stringify postings cache key": {
			key:      cacheKey{uid, cacheKeyPostings(labels.Label{Name: "foo", Value: "bar"})},
			expected: fmt.Sprintf("P:%s:foo:bar", uid.String()),
		},
		"should stringify series cache key": {
			key:      cacheKey{uid, cacheKeySeries(12345)},
			expected: fmt.Sprintf("S:%s:12345", uid.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.key.string()
			testutil.Equals(t, testData.expected, actual)
		})
	}
}
