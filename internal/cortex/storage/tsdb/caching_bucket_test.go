package tsdb

import (
	"fmt"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestIsTenantDir(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.True(t, isTenantBlocksDir("test"))
	assert.True(t, isTenantBlocksDir("test/"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
}

func TestIsBucketIndexFile(t *testing.T) {
	assert.False(t, isBucketIndexFile(""))
	assert.False(t, isBucketIndexFile("test"))
	assert.False(t, isBucketIndexFile("test/block"))
	assert.False(t, isBucketIndexFile("test/block/chunks"))
	assert.True(t, isBucketIndexFile("test/bucket-index.json.gz"))
}

func TestIsBlockIndexFile(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	assert.False(t, isBlockIndexFile(""))
	assert.False(t, isBlockIndexFile("/index"))
	assert.False(t, isBlockIndexFile("test/index"))
	assert.False(t, isBlockIndexFile("/test/index"))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("%s/index", blockID.String())))
	assert.True(t, isBlockIndexFile(fmt.Sprintf("/%s/index", blockID.String())))
}
