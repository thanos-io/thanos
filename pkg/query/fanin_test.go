package query

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestDedupStrings(t *testing.T) {
	str := []string{"f", "b", "c", "b", "a", "f", "f"}

	testutil.Equals(t, []string{"a", "b", "c", "f"}, dedupStrings(str))
}
