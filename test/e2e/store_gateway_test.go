package e2e_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/promclient"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
	yaml "gopkg.in/yaml.v2"
)

func TestStoreGatewayQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	s3Config := s3.Config{
		Bucket:    "test-storegateway-query",
		AccessKey: "abc",
		SecretKey: "mightysecret",
		Endpoint:  minioHTTP(1),
		Insecure:  true,
	}

	bucketConfig := client.BucketConfig{
		Type:   client.S3,
		Config: s3Config,
	}

	config, err := yaml.Marshal(bucketConfig)
	testutil.Ok(t, err)

	exit, err := newSpinupSuite().
		WithPreStartedMinio(s3Config).
		Add(storeGateway(1, config)).
		Add(querier(1, "replica", storeGatewayGRPC(1))).
		Exec(t, ctx, "test_store_gateway_query")
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
	id1, err := testutil.CreateBlock(dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset, 0)
	testutil.Ok(t, err)

	id2, err := testutil.CreateBlock(dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset2, 0)
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

		var err error
		res, err = promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+queryHTTP(1)), "{a=\"1\"}", time.Now(), false)
		if err != nil {
			return err
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

		var err error
		res, err = promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+queryHTTP(1)), "{a=\"1\"}", time.Now(), true)
		if err != nil {
			return err
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
