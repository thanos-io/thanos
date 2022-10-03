package benchmarks

import (
	"fmt"
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
	tracingclient "github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"gopkg.in/yaml.v2"
)

const bktName = "test"

func marshal(t testing.TB, i interface{}) []byte {
	t.Helper()

	b, err := yaml.Marshal(i)
	testutil.Ok(t, err)

	return b
}

const (
	blockMaxTime = 1568851200000
	initialView  = "/graph?g0.expr=count(cluster_version)&g0.tab=0&g0.stacked=0&g0.range_input=2d&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2019-09-19%2000%3A00%3A00&g0.moment_input=2019-09-19%2000%3A00%3A00&g0.step_input=700&g1.expr=count(count_over_time(cluster_version%5B2d%5D))&g1.tab=1&g1.stacked=0&g1.range_input=1h&g1.max_source_resolution=0s&g1.deduplicate=1&g1.partial_response=0&g1.store_matches=%5B%5D&g1.end_input=2019-09-19%2000%3A00%3A00&g1.moment_input=2019-09-19%2000%3A00%3A00"
)

func TestQueryStore_Macrobench(t *testing.T) {
	pwd, err := os.Getwd()
	testutil.Ok(t, err)

	// NOTE(bwplotka): Only available on my machine due to size of blocks. Replace with your local (or remove blocks).
	// My setup:
	// One block used and symlinked as four different blocks. Two in A dir, and two in B dir.
	//  ...
	//  "minTime": 1567641600000,
	//	"maxTime": 1568851200000, (2 weeks)
	//	"stats": {
	//		"numSamples": 5397517846,
	//		"numSeries": 8377876,
	//		"numChunks": 67874256
	//	},
	//	"compaction": {
	//		"level": 4
	// 	...
	//
	bktDir := filepath.Join(pwd, "tsdb/bucket")
	e, err := e2e.New(e2e.WithVolumes(fmt.Sprintf("%v:%v:z", filepath.Join(pwd, "tsdb"), filepath.Join(pwd, "tsdb"))))
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	// Start monitoring.
	mon, err := e2emon.Start(e)
	testutil.Ok(t, err)
	testutil.Ok(t, mon.OpenUserInterfaceInBrowser())

	// Start profiling.
	prof, err := e2eprof.Start(e)
	testutil.Ok(t, err)
	testutil.Ok(t, prof.OpenUserInterfaceInBrowser())

	// Start tracing.
	j := e.Runnable("tracing").WithPorts(map[string]int{"http-front": 16686, "jaeger.thrift": 14268}).Init(e2e.StartOptions{Image: "jaegertracing/all-in-one:1.25"})
	testutil.Ok(t, e2e.StartAndWaitReady(j))
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+j.Endpoint("http-front")))

	jaegerConfig, err := yaml.Marshal(tracingclient.TracingConfig{
		Type: tracingclient.Jaeger,
		Config: jaeger.Config{
			ServiceName:  "thanos",
			SamplerType:  "const",
			SamplerParam: 1,
			Endpoint:     "http://" + j.InternalEndpoint("jaeger.thrift") + "/api/traces",
		},
	})
	testutil.Ok(t, err)

	// Setup with experimental PromQL (fallback turned off).
	var querierExpPromQL e2e.Runnable
	{
		const image = "thanos:02-09-22-after-fix" // "thanos:pr5742"
		storeA := e2edb.NewThanosStore(e, "store-exppromql-A", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(bktDir, "A"),
			},
		}), e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config": string(jaegerConfig),
		}))
		storeB := e2edb.NewThanosStore(e, "store-exppromql-B", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(bktDir, "B"),
			},
		}), e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config": string(jaegerConfig),
		}))
		querierExpPromQL = e2edb.NewThanosQuerier(e, "query-exppromql", []string{
			storeA.InternalEndpoint("grpc"),
			storeB.InternalEndpoint("grpc"),
		}, e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config":      string(jaegerConfig),
			"--query.promql-engine": "thanos",
		}))
		testutil.Ok(t, e2e.StartAndWaitReady(storeA, storeB, querierExpPromQL))
	}

	// Setup with normal PromQL (pre k-way merge).
	var querier e2e.Runnable
	{
		const image = "thanos:pre-k-way"
		storeA := e2edb.NewThanosStore(e, "store-A", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(bktDir, "A"),
			},
		}), e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config": string(jaegerConfig),
		}))
		storeB := e2edb.NewThanosStore(e, "store-B", marshal(t, client.BucketConfig{
			Type: client.FILESYSTEM,
			Config: filesystem.Config{
				Directory: filepath.Join(bktDir, "B"),
			},
		}), e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config": string(jaegerConfig),
		}))
		querier = e2edb.NewThanosQuerier(e, "query", []string{
			storeA.InternalEndpoint("grpc"),
			storeB.InternalEndpoint("grpc"),
		}, e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
			"--tracing.config": string(jaegerConfig),
		}))
		testutil.Ok(t, e2e.StartAndWaitReady(storeA, storeB, querier))
	}

	// Initial load test.
	k6 := e.Runnable("k6").Init(e2e.StartOptions{
		Command: e2e.NewCommandRunUntilStop(),
		Image:   "grafana/k6:0.39.0",
	})
	testutil.Ok(t, e2e.StartAndWaitReady(k6))

	exprURL := fmt.Sprintf("http://%v/%v",
		querierExpPromQL.InternalEndpoint("http"),
		"api/v1/query_range?query=count%28cluster_version%29&dedup=true&"+
			"partial_response=false&start=1568678400&end=1568851200&step=700&max_source_resolution=0s")

	testutil.Ok(t, k6.Exec(e2e.NewCommand(
		"/bin/sh", "-c",
		`cat << EOF | k6 run -u 1 -i 5 -
import http from 'k6/http';
import { check, sleep } from 'k6';

export default function () {
	const res = http.get('`+exprURL+`');
	check(res, {
		'is status 200': (r) => r.status === 200,
	});
}
EOF`)))

	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querierExpPromQL.Endpoint("http")+initialView))
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit()) // Debug.
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querier.Endpoint("http")+initialView))

	// Once done, wait for user input so user can explore the results in Prometheus UI and logs.
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
}
