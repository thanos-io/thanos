package benchmarks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2einteractive "github.com/efficientgo/e2e/interactive"
	e2emon "github.com/efficientgo/e2e/monitoring"
	e2eprof "github.com/efficientgo/e2e/profiling"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"
)

const bktName = "test"

func marshal(t testing.TB, i interface{}) []byte {
	t.Helper()

	b, err := yaml.Marshal(i)
	testutil.Ok(t, err)

	return b
}

const blockMaxTime = 1568851200000

func TestQueryStore_Macrobench(t *testing.T) {
	e, err := e2e.New()
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	// Start monitoring.
	mon, err := e2emon.Start(e)
	testutil.Ok(t, err)
	testutil.Ok(t, mon.OpenUserInterfaceInBrowser())

	// Start monitoring.
	prof, err := e2eprof.Start(e)
	testutil.Ok(t, err)
	testutil.Ok(t, prof.OpenUserInterfaceInBrowser())

	k6 := e.Runnable("k6").Init(e2e.StartOptions{
		Command: e2e.NewCommandRunUntilStop(),
		Image:   "grafana/k6:0.39.0",
	})
	testutil.Ok(t, e2e.StartAndWaitReady(k6))

	pwd, err := os.Getwd()
	testutil.Ok(t, err)
	testutil.Ok(t, os.Symlink(filepath.Join(pwd, "tsdb/bucket"), filepath.Join(k6.Dir(), "bucket")))

	// Setup with experimental PromQL (fallback turned off).
	var querierExpPromQL e2e.Runnable
	{
		storeA := e2edb.NewThanosStore(e, "store-exppromql-A", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(k6.InternalDir(), "bucket/A"),
			},
		}), e2edb.WithImage("thanos:latest"))
		storeB := e2edb.NewThanosStore(e, "store-exppromql-B", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(k6.InternalDir(), "bucket/B"),
			},
		}), e2edb.WithImage("thanos:latest"))
		querierExpPromQL = e2edb.NewThanosQuerier(e, "query-exppromql", []string{
			storeA.InternalEndpoint("grpc"),
			storeB.InternalEndpoint("grpc"),
		}, e2edb.WithImage("thanos:latest"), e2edb.WithFlagOverride(map[string]string{
			"--query.promql-engine": "thanos",
		}))
		testutil.Ok(t, e2e.StartAndWaitReady(storeA, storeB, querierExpPromQL))
	}
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querierExpPromQL.Endpoint("http")))

	// Setup with normal PromQL.
	var querier e2e.Runnable
	{
		storeA := e2edb.NewThanosStore(e, "store-A", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(k6.InternalDir(), "bucket/A"),
			},
		}), e2edb.WithImage("thanos:latest"))
		storeB := e2edb.NewThanosStore(e, "store-B", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(k6.InternalDir(), "bucket/B"),
			},
		}), e2edb.WithImage("thanos:latest"))
		querier = e2edb.NewThanosQuerier(e, "query", []string{
			storeA.InternalEndpoint("grpc"),
			storeB.InternalEndpoint("grpc"),
		}, e2edb.WithImage("thanos:latest"))
		testutil.Ok(t, e2e.StartAndWaitReady(storeA, storeB, querier))
	}
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querier.Endpoint("http")))

	//	// Load test labeler from 1 clients with k6 and export result to Prometheus.
	//	k6 := e.Runnable("k6").Init(e2e.StartOptions{
	//		Command: e2e.NewCommandRunUntilStop(),
	//		Image:   "grafana/k6:0.39.0",
	//	})
	//	testutil.Ok(t, e2e.StartAndWaitReady(k6))
	//
	//	url := fmt.Sprintf(
	//		"http://%s/label_object?object_id=object1.txt",
	//		labeler.InternalEndpoint("http"),
	//	)
	//	testutil.Ok(t, k6.Exec(e2e.NewCommand(
	//		"/bin/sh", "-c",
	//		`cat << EOF | k6 run -u 1 -d 5m -
	//import http from 'k6/http';
	//import { check, sleep } from 'k6';
	//
	//export default function () {
	//	const res = http.get('`+url+`');
	//	check(res, {
	//		'is status 200': (r) => r.status === 200,
	//		'response': (r) =>
	//			r.body.includes('{"object_id":"object1.txt","sum":6221600000,"checksum":"SUU'),
	//	});
	//	sleep(0.5)
	//}
	//EOF`)))

	// Once done, wait for user input so user can explore the results in Prometheus UI and logs.
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
}
