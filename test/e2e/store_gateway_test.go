package e2e_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	yaml "gopkg.in/yaml.v2"
)

func TestStoreGateway(t *testing.T) {
	a := newLocalAddresser()
	minioAddr := a.New()

	s3Config := s3.Config{
		Bucket:    "test-storegateway-query",
		AccessKey: "abc",
		SecretKey: "mightysecret",
		Endpoint:  minioAddr.HostPort(),
		Insecure:  true,
	}

	bucketConfig := client.BucketConfig{
		Type:   client.S3,
		Config: s3Config,
	}

	config, err := yaml.Marshal(bucketConfig)
	testutil.Ok(t, err)

	s := storeGateway(a.New(), a.New(), config)
	q := querier(a.New(), a.New(), []address{s.GRPC}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	exit, err := e2eSpinupWithS3ObjStorage(t, ctx, minioAddr, &s3Config, s, q)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	dir, err := ioutil.TempDir("", "test_store_gateway_query_local")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "2"),
	}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")
	extLset2 := labels.FromStrings("ext1", "value1", "replica", "2")

	now := time.Now()
	id1, err := testutil.CreateBlock(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset, 0)
	testutil.Ok(t, err)

	id2, err := testutil.CreateBlock(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset2, 0)
	testutil.Ok(t, err)

	l := log.NewLogfmtLogger(os.Stdout)

	bkt, err := s3.NewBucketWithConfig(l, s3Config, "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))

	var res model.Vector

	// Try query without deduplication.
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		var (
			err      error
			warnings []string
		)
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "{a=\"1\"}", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != 2 {
			return errors.Errorf("unexpected result size %d", len(res))
		}
		return nil
	}))

	// In our model result are always sorted.
	testutil.Equals(t, model.Metric{
		"a":       "1",
		"b":       "2",
		"ext1":    "value1",
		"replica": "1",
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"a":       "1",
		"b":       "2",
		"ext1":    "value1",
		"replica": "2",
	}, res[1].Metric)

	// Try query with deduplication.
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		var (
			err      error
			warnings []string
		)
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "{a=\"1\"}", time.Now(), promclient.QueryOptions{
			Deduplicate: true,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != 1 {
			return errors.Errorf("unexpected result size %d", len(res))
		}
		return nil
	}))

	// In our model result are always sorted.
	testutil.Equals(t, model.Metric{
		"a":    "1",
		"b":    "2",
		"ext1": "value1",
	}, res[0].Metric)
}
