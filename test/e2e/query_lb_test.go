package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2einteractive "github.com/efficientgo/e2e/interactive"
	e2emon "github.com/efficientgo/e2e/monitoring"
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

// TestQuery_WithStores_Loadbalancing is testing.
// * Create & Start Querier
// * Prepare "object storage" (test hack: It can just filesystem).
//   - Create one TSDB block.
//
// * Create & Start 2x Stores
// * Connect Querier with Stores (tricky - there is no way of marking store as LB...)
// * Assertion: Monitor the traffic distribution.
func TestQuery_WithStores_Loadbalancing(t *testing.T) {
	pwd, err := os.Getwd()
	testutil.Ok(t, err)

	// Create a local dir that will be shared with containers with TSDB blocks we need.
	// TODO(bwplotka): Create a block here (e.g using thanosbench).
	bktDir := filepath.Join(pwd, "tsdb/bucket")
	e, err := e2e.New(
		e2e.WithVolumes(
			fmt.Sprintf("%v:%v:z", filepath.Join(pwd, "tsdb"), filepath.Join(pwd, "tsdb"))),
	)
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	// Start monitoring.
	mon, err := e2emon.Start(e)
	testutil.Ok(t, err)
	testutil.Ok(t, mon.OpenUserInterfaceInBrowser())

	// Start tracing.
	j := e.Runnable("tracing").WithPorts(map[string]int{"http-front": 16686, "jaeger.thrift": 14268}).Init(e2e.StartOptions{Image: "jaegertracing/all-in-one:1.25"})
	testutil.Ok(t, e2e.StartAndWaitReady(j))
	//testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+j.Endpoint("http-front")))

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

	const thanosImage = "thanos:latest" // Run 'make thanos' in thanos root to recreate it.
	store1 := e2edb.NewThanosStore(e, "store1", marshal(t, client.BucketConfig{
		Type: client.FILESYSTEM,
		Config: filesystem.Config{
			Directory: bktDir,
		},
	}), e2edb.WithImage(thanosImage), e2edb.WithFlagOverride(map[string]string{
		"--tracing.config": string(jaegerConfig),
	}))
	store2 := e2edb.NewThanosStore(e, "store2", marshal(t, client.BucketConfig{
		Type: client.FILESYSTEM,
		Config: filesystem.Config{
			Directory: bktDir,
		},
	}), e2edb.WithImage(thanosImage), e2edb.WithFlagOverride(map[string]string{
		"--tracing.config": string(jaegerConfig),
	}))
	querier := e2edb.NewThanosQuerier(e, "query", []string{
		// TODO(bwplotka): Play with loadbalancing to ensure half of requests goes to store1 and half to store2.
		store1.InternalEndpoint("grpc"),
		store2.InternalEndpoint("grpc"),
	}, e2edb.WithImage(thanosImage), e2edb.WithFlagOverride(map[string]string{
		"--tracing.config": string(jaegerConfig),
	}))
	testutil.Ok(t, e2e.StartAndWaitReady(store1, store2, querier))

	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+querier.Endpoint("http")))

	// Once done, wait for user input so user can explore
	// the results in Prometheus UI and logs.
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
}
