package block

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"

	"github.com/oklog/ulid"
)

func TestIsBlockDir(t *testing.T) {
	for _, tc := range []struct {
		input string
		id    ulid.ULID
		bdir  bool
	}{
		{
			input: "",
			bdir:  false,
		},
		{
			input: "something",
			bdir:  false,
		},
		{
			id:    ulid.MustNew(1, nil),
			input: ulid.MustNew(1, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(2, nil),
			input: "/" + ulid.MustNew(2, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(3, nil),
			input: "some/path/" + ulid.MustNew(3, nil).String(),
			bdir:  true,
		},
		{
			input: ulid.MustNew(4, nil).String() + "/something",
			bdir:  false,
		},
	} {
		t.Run(tc.input, func(t *testing.T) {
			id, ok := IsBlockDir(tc.input)
			testutil.Equals(t, tc.bdir, ok)

			if id.Compare(tc.id) != 0 {
				t.Errorf("expected %s got %s", tc.id, id)
				t.FailNow()
			}
		})
	}
}

func TestUpload(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-block-upload")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt := inmem.NewBucket()
	b1, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String()), os.ModePerm))

	{
		// Wrong dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "not-existing"))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/not-existing: no such file or directory"), "")
	}
	{
		// Wrong existing dir (not a block).
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test"))
		testutil.NotOk(t, err)
		testutil.Equals(t, "not a block dir: ulid: bad data size when unmarshaling", err.Error())
	}
	{
		// Empty block dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/meta.json: no such file or directory"), "")
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Missing chunks.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/chunks: no such file or directory"), "")

		// Only debug meta.json present.
		testutil.Equals(t, 1, len(bkt.Objects()))
	}
	testutil.Ok(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String(), ChunksDirname), os.ModePerm))
	testutil.Copy(t, path.Join(tmpDir, b1.String(), ChunksDirname, "000001"), path.Join(tmpDir, "test", b1.String(), ChunksDirname, "000001"))
	{
		// Missing index file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/index: no such file or directory"), "")

		// Only debug meta.json present.
		testutil.Equals(t, 1, len(bkt.Objects()))
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), IndexFilename), path.Join(tmpDir, "test", b1.String(), IndexFilename))
	testutil.Ok(t, os.Remove(path.Join(tmpDir, "test", b1.String(), MetaFilename)))
	{
		// Missing meta.json file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()))
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/meta.json: no such file or directory"), "")

		// Only debug meta.json present.
		testutil.Equals(t, 1, len(bkt.Objects()))
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Full block.
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String())))
		testutil.Equals(t, 4, len(bkt.Objects()))
		testutil.Equals(t, 3751, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		testutil.Equals(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		testutil.Equals(t, 365, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Test Upload is idempotent.
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String())))
		testutil.Equals(t, 4, len(bkt.Objects()))
		testutil.Equals(t, 3751, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		testutil.Equals(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		testutil.Equals(t, 365, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Upload with no external labels should be blocked.
		b2, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, nil, 124)
		testutil.Ok(t, err)
		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()))
		testutil.NotOk(t, err)
		testutil.Equals(t, "empty external labels are not allowed for Thanos block.", err.Error())
		testutil.Equals(t, 4, len(bkt.Objects()))
	}
}

func TestDelete(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-block-delete")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt := inmem.NewBucket()
	{
		b1, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124)
		testutil.Ok(t, err)
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b1.String())))
		testutil.Equals(t, 4, len(bkt.Objects()))

		// Full delete.
		testutil.Ok(t, Delete(ctx, log.NewNopLogger(), bkt, b1))
		// Still debug meta entry is expected.
		testutil.Equals(t, 1, len(bkt.Objects()))
	}
	{
		b2, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124)
		testutil.Ok(t, err)
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String())))
		testutil.Equals(t, 5, len(bkt.Objects()))

		// Remove meta.json and check if delete can delete it.
		testutil.Ok(t, bkt.Delete(ctx, path.Join(b2.String(), MetaFilename)))
		testutil.Ok(t, Delete(ctx, log.NewNopLogger(), bkt, b2))
		// Still 2 debug meta entries are expected.
		testutil.Equals(t, 2, len(bkt.Objects()))
	}
}
