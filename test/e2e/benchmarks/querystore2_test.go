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

func marshal(t testing.TB, i interface{}) []byte {
	t.Helper()

	b, err := yaml.Marshal(i)
	testutil.Ok(t, err)

	return b
}

const (
	initialView = "/graph?g0.expr=count(cluster_version)&g0.tab=0&g0.stacked=0&g0.range_input=2d&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2019-09-19%2000%3A00%3A00&g0.moment_input=2019-09-19%2000%3A00%3A00&g0.step_input=700&g1.expr=count(count_over_time(cluster_version%5B2d%5D))&g1.tab=1&g1.stacked=0&g1.range_input=1h&g1.max_source_resolution=0s&g1.deduplicate=1&g1.partial_response=0&g1.store_matches=%5B%5D&g1.end_input=2019-09-19%2000%3A00%3A00&g1.moment_input=2019-09-19%2000%3A00%3A00"
	demoNewView = "/graph?g0.expr=%7Bis%3D\"thanos-with-new-promql-enabled\"%7D&g0.tab=1&g0.stacked=0&g0.range_input=2d&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2019-09-19%2000%3A00%3A00&g0.moment_input=2019-09-19%2000%3A00%3A00&g0.step_input=700&g1.expr=count(cluster_version)-&g1.tab=0&g1.stacked=0&g1.range_input=1d&g1.max_source_resolution=0s&g1.deduplicate=1&g1.partial_response=0&g1.store_matches=%5B%5D&g1.end_input=2019-09-18%2012%3A00%3A00&g1.moment_input=2019-09-18%2012%3A00%3A00&g1.step_input=300&g2.expr=count(cluster_version%7Bfrom_version!%3D\"4.1.9\"%7D)%20%2F%20count(cluster_version)-&g2.tab=0&g2.stacked=0&g2.range_input=1d&g2.max_source_resolution=0s&g2.deduplicate=1&g2.partial_response=0&g2.store_matches=%5B%5D&g2.end_input=2019-09-18%2012%3A00%3A00&g2.moment_input=2019-09-18%2012%3A00%3A00&g2.step_input=300"
	demoOldView = "/graph?g0.expr=%7Bis%3D\"thanos-with-old-promql\"%7D&g0.tab=1&g0.stacked=0&g0.range_input=2d&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2019-09-19%2000%3A00%3A00&g0.moment_input=2019-09-19%2000%3A00%3A00&g0.step_input=700&g1.expr=count(cluster_version)-&g1.tab=0&g1.stacked=0&g1.range_input=1d&g1.max_source_resolution=0s&g1.deduplicate=1&g1.partial_response=0&g1.store_matches=%5B%5D&g1.end_input=2019-09-18%2012%3A00%3A00&g1.moment_input=2019-09-18%2012%3A00%3A00&g1.step_input=300&g2.expr=count(cluster_version%7Bfrom_version!%3D\"4.1.9\"%7D)%20%2F%20count(cluster_version)-&g2.tab=0&g2.stacked=0&g2.range_input=1d&g2.max_source_resolution=0s&g2.deduplicate=1&g2.partial_response=0&g2.store_matches=%5B%5D&g2.end_input=2019-09-18%2012%3A00%3A00&g2.moment_input=2019-09-18%2012%3A00%3A00&g2.step_input=300"
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
	testutil.Ok(t, mon.OpenUserInterfaceInBrowser("/graph?g0.expr=sum(rate(container_cpu_usage_seconds_total%7Bname%3D~\".*query.*%7C.*store.*\"%7D%5B1m%5D))%20by%20(name)&g0.tab=0&g0.stacked=0&g0.show_exemplars=0&g0.range_input=30m&g1.expr=go_memstats_alloc_bytes%7Bjob%3D~\"query.*%7Cstore.*\"%7D&g1.tab=0&g1.stacked=0&g1.show_exemplars=0&g1.range_input=30m"))
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

	const image = "thanos:demo-3-11-22-v2"
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

	// Querier with experimental PromQL turned on (fallback turned off).
	querierExpPromQL := e2edb.NewThanosQuerier(e, "query-exppromql", []string{
		storeA.InternalEndpoint("grpc"),
		storeB.InternalEndpoint("grpc"),
	}, e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
		"--tracing.config":      string(jaegerConfig),
		"--query.promql-engine": "thanos",
	}))

	// Setup with normal PromQL (pre k-way merge).
	querier := e2edb.NewThanosQuerier(e, "query", []string{
		storeA.InternalEndpoint("grpc"),
		storeB.InternalEndpoint("grpc"),
	}, e2edb.WithImage(image), e2edb.WithFlagOverride(map[string]string{
		"--tracing.config": string(jaegerConfig),
	}))
	testutil.Ok(t, e2e.StartAndWaitReady(storeA, storeB, querier, querierExpPromQL))

	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querierExpPromQL.Endpoint("http")+demoNewView))
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querier.Endpoint("http")+demoOldView))

	// Once done, wait for user input so user can explore the results in Prometheus UI and logs.
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
}
