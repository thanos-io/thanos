package compact

import (
	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"testing"
)

func TestFilterNilCompact(t *testing.T) {
	blocks := []*metadata.Meta{nil, nil}
	filtered := FilterNilBlocks(blocks)
	testutil.Equals(t, 0, len(filtered))
}
