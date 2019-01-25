package shipper

import (
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb"
)

func TestShipperTimestamps(t *testing.T) {
	dir, err := ioutil.TempDir("", "shipper-test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	s := New(nil, nil, dir, nil, nil, metadata.TestSource)

	// Missing thanos meta file.
	_, _, err = s.Timestamps()
	testutil.NotOk(t, err)

	meta := &Meta{Version: 1}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), dir, meta))

	// Nothing uploaded, nothing in the filesystem. We assume that
	// we are still waiting for TSDB to dump first TSDB block.
	mint, maxt, err := s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(0), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id1 := ulid.MustNew(1, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id1.String()), &metadata.Meta{
		Version: 1,
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
		},
	}))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	id2 := ulid.MustNew(2, nil)
	testutil.Ok(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), path.Join(dir, id2.String()), &metadata.Meta{
		Version: 1,
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 4000,
			MinTime: 2000,
		},
	}))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(math.MinInt64), maxt)

	meta = &Meta{
		Version:  1,
		Uploaded: []ulid.ULID{id1},
	}
	testutil.Ok(t, WriteMetaFile(log.NewNopLogger(), dir, meta))
	mint, maxt, err = s.Timestamps()
	testutil.Ok(t, err)
	testutil.Equals(t, int64(1000), mint)
	testutil.Equals(t, int64(2000), maxt)
}
