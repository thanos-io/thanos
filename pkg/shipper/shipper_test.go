package shipper

import (
	"testing"

	"context"
	"io/ioutil"
	"time"

	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/promlts/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

type inMemStorage struct {
	t    *testing.T
	dirs map[string]struct{}
}

func newInMemStorage(t *testing.T) *inMemStorage {
	return &inMemStorage{
		t:    t,
		dirs: make(map[string]struct{}),
	}
}

func (r *inMemStorage) Exists(_ context.Context, id string) (bool, error) {
	_, exists := r.dirs[id]
	return exists, nil
}

func (r *inMemStorage) Upload(_ context.Context, dir string) error {
	// Double check if shipper checks Exists method properly.
	_, exists := r.dirs[dir]
	testutil.Assert(r.t, !exists, "target should not exists")

	r.dirs[dir] = struct{}{}
	return nil
}

func TestShipper_UploadsBlocksFromProm(t *testing.T) {
	logger := log.NewNopLogger()

	dir, err := ioutil.TempDir("", "shipper-test-snapshots")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	p, err := testutil.NewTSDB()
	testutil.Ok(t, err)
	defer p.Close()

	storage := newInMemStorage(t)
	shipper := New(
		logger,
		nil,
		dir,
		storage,
		IsULIDDir,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start shipping blocks.
	go func() { testutil.Ok(t, shipper.Run(ctx)) }()

	baseT := timestamp.FromTime(time.Now())

	// Produce 10 different blocks and wait max 5 seconds for shipper to ship them.
	for i := int64(0); i < 10; i++ {
		ctx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()

		// Add fake intput.
		a := p.Appender()

		// Make labels, timestamp and values unique.
		labelVal := fmt.Sprintf("b%v", i)
		baseT := baseT + (i * 300)
		baseV := float64(i * 3)

		a.Add(labels.FromStrings("a", labelVal), baseT+100, baseV+1)
		a.Add(labels.FromStrings("a", labelVal), baseT+200, baseV+2)
		a.Add(labels.FromStrings("a", labelVal), baseT+300, baseV+3)
		testutil.Ok(t, a.Commit())

		testutil.Ok(t, p.Snapshot(dir))
		waitForShipment(t, ctx, storage, len(p.Blocks()))
	}

	blocks := p.Blocks()
	// Check if storage matches with that tsdb claims to have at the end.
	testutil.Equals(t, len(storage.dirs), len(blocks))
	for _, b := range blocks {
		_, exists := storage.dirs[b.Dir()]
		testutil.Assert(t, exists, "did not found block %s in shipped data", b.Dir())
	}
}

func waitForShipment(t *testing.T, ctx context.Context, storage *inMemStorage, expectedBlocks int) {
	for {
		if ctx.Err() != nil {
			t.Errorf("context cancelled. not seen expected blocks (%d) in storage in time", expectedBlocks)
			t.FailNow()
		}

		if len(storage.dirs) >= expectedBlocks {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
