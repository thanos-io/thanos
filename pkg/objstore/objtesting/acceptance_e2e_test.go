package objtesting

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

// TestObjStoreAcceptanceTest_e2e tests all known implementation against interface behaviour contract we agreed on.
// This ensures consistent behaviour across all implementations.
// NOTE: This test assumes strong consistency, but in the same way it does not guarantee that if it passes, the
// used object store is strongly consistent.
func TestObjStore_AcceptanceTest_e2e(t *testing.T) {
	ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		// Create temporary dir.
		dir, err := ioutil.TempDir("", "test_bucket_e2e_local")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		_, err = bkt.Get(context.Background(), "")
		testutil.NotOk(t, err)
		testutil.Assert(t, !bkt.IsObjNotFoundErr(err), "expected user error got not found %s", err)

		_, err = bkt.Get(context.Background(), "id1/obj_1.some")
		testutil.NotOk(t, err)
		testutil.Assert(t, bkt.IsObjNotFoundErr(err), "expected not found error got %s", err)

		ok, err := bkt.Exists(context.Background(), "id1/obj_1.some")
		testutil.Ok(t, err)
		testutil.Assert(t, !ok, "expected not exits")

		obj1, err := createTempObject(dir, "obj_1.some", "@test-data@")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, obj1.Close()) }()

		// Upload first object.
		testutil.Ok(t, bkt.Upload(context.Background(), "id1/obj_1.some", obj1))

		// Double check we can immediately read it.
		rc1, err := bkt.Get(context.Background(), "id1/obj_1.some")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, rc1.Close()) }()
		content, err := ioutil.ReadAll(rc1)
		testutil.Ok(t, err)
		testutil.Equals(t, "@test-data@", string(content))

		rc2, err := bkt.GetRange(context.Background(), "id1/obj_1.some", 1, 3)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, rc2.Close()) }()
		content, err = ioutil.ReadAll(rc2)
		testutil.Ok(t, err)
		testutil.Equals(t, "tes", string(content))

		ok, err = bkt.Exists(context.Background(), "id1/obj_1.some")
		testutil.Ok(t, err)
		testutil.Assert(t, ok, "expected exits")

		// Upload other objects.
		obj2, err := createTempObject(dir, "obj_2.some", "@test-data2@")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, obj2.Close()) }()
		testutil.Ok(t, bkt.Upload(context.Background(), "id1/obj_2.some", obj2))

		obj3, err := createTempObject(dir, "obj_3.some", "@test-data3@")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, obj3.Close()) }()
		testutil.Ok(t, bkt.Upload(context.Background(), "id1/obj_3.some", obj3))

		obj4, err := createTempObject(dir, "obj_4.some", "@test-data4@")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, obj4.Close()) }()
		testutil.Ok(t, bkt.Upload(context.Background(), "id2/obj_4.some", obj4))

		obj5, err := createTempObject(dir, "obj_5.some", "@test-data5@")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, obj5.Close()) }()
		testutil.Ok(t, bkt.Upload(context.Background(), "obj_5.some", obj5))

		// Can we iter over items from top dir?
		var seen []string
		testutil.Ok(t, bkt.Iter(context.Background(), "", func(fn string) error {
			seen = append(seen, fn)
			return nil
		}))
		testutil.Equals(t, []string{"obj_5.some", "id1/", "id2/"}, seen)

		// Can we iter over items from id1/ dir?
		seen = []string{}
		testutil.Ok(t, bkt.Iter(context.Background(), "id1/", func(fn string) error {
			seen = append(seen, fn)
			return nil
		}))
		testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some"}, seen)

		// Can we iter over items from id1 dir?
		seen = []string{}
		testutil.Ok(t, bkt.Iter(context.Background(), "id1", func(fn string) error {
			seen = append(seen, fn)
			return nil
		}))
		testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some"}, seen)

		// Can we iter over items from not existing dir?
		testutil.Ok(t, bkt.Iter(context.Background(), "id0", func(fn string) error {
			t.Error("Not expected to loop through not existing directory")
			t.FailNow()

			return nil
		}))

		testutil.Ok(t, bkt.Delete(context.Background(), "id1/obj_2.some"))

		// Can we iter over items from id1/ dir and see obj2 being deleted?
		seen = []string{}
		testutil.Ok(t, bkt.Iter(context.Background(), "id1/", func(fn string) error {
			seen = append(seen, fn)
			return nil
		}))
		testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_3.some"}, seen)

		testutil.Ok(t, objstore.DeleteDir(context.Background(), bkt, "id1"))
		testutil.Ok(t, bkt.Iter(context.Background(), "id1/", func(fn string) error {
			t.Error("Not expected to loop through empty / non existing directory")
			t.FailNow()

			return nil
		}))
	})
}

func createTempObject(dir, name, content string) (*os.File, error) {
	tmpfile, err := ioutil.TempFile(dir, name)
	if err != nil {
		return nil, err
	}

	if _, err := tmpfile.WriteString(content); err != nil {
		return nil, err
	}

	// To set the offset to the origin of the temporary file.
	if _, err := tmpfile.Seek(0, 0); err != nil {
		return nil, err
	}
	return tmpfile, nil
}
