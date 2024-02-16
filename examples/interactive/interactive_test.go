// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ebench_test

import (
	"fmt"
	"os"
	execlib "os/exec"
	"path/filepath"
	"testing"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2einteractive "github.com/efficientgo/e2e/interactive"
	e2emon "github.com/efficientgo/e2e/monitoring"
	e2eprof "github.com/efficientgo/e2e/profiling"

	"github.com/efficientgo/e2e/monitoring/promconfig"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"
	tracingclient "github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const (
	data           = "data"
	defaultProfile = "continuous-30d-tiny"
)

var (
	maxTimeFresh = `2021-07-27T00:00:00Z`
	maxTimeOld   = `2021-07-20T00:00:00Z`

	store1Data = func() string { a, _ := filepath.Abs(filepath.Join(data, "store1")); return a }()
	store2Data = func() string { a, _ := filepath.Abs(filepath.Join(data, "store2")); return a }()
	prom1Data  = func() string { a, _ := filepath.Abs(filepath.Join(data, "prom1")); return a }()
	prom2Data  = func() string { a, _ := filepath.Abs(filepath.Join(data, "prom2")); return a }()
)

func exec(cmd string, args ...string) error {
	if o, err := execlib.Command(cmd, args...).CombinedOutput(); err != nil {
		return errors.Wrap(err, string(o))
	}
	return nil
}

// createData generates some blocks for us to play with and makes them
// available to store and Prometheus instances.
//
// You can choose different profiles by setting the BLOCK_PROFILE environment variable.
// Available profiles can be found at https://github.com/thanos-io/thanosbench/blob/master/pkg/blockgen/profiles.go#L28
func createData() (perr error) {
	profile := os.Getenv("BLOCK_PROFILE")
	if profile == "" {
		profile = defaultProfile //	Use "continuous-1w-1series-10000apps" if you have ~10GB of memory for creation phase.
	}

	fmt.Println("Re-creating data (can take minutes)...")
	defer func() {
		if perr != nil {
			_ = os.RemoveAll(data)
		}
	}()

	if err := exec(
		"sh", "-c",
		fmt.Sprintf("mkdir -p %s && "+
			"docker run -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block plan -p %s --labels 'cluster=\"eu-1\"' --labels 'replica=\"0\"' --max-time=%s | "+
			"docker run -v %s/:/shared -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block gen --output.dir /shared", store1Data, profile, maxTimeOld, store1Data),
	); err != nil {
		return err
	}
	if err := exec(
		"sh", "-c",
		fmt.Sprintf("mkdir -p %s && "+
			"docker run -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block plan -p %s --labels 'cluster=\"us-1\"' --labels 'replica=\"0\"' --max-time=%s | "+
			"docker run -v %s/:/shared -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block gen --output.dir /shared", store2Data, profile, maxTimeOld, store2Data),
	); err != nil {
		return err
	}

	if err := exec(
		"sh", "-c",
		fmt.Sprintf("mkdir -p %s && "+
			"docker run -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block plan -p %s --max-time=%s | "+
			"docker run -v %s/:/shared -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block gen --output.dir /shared", prom1Data, profile, maxTimeFresh, prom1Data),
	); err != nil {
		return err
	}
	if err := exec(
		"sh", "-c",
		fmt.Sprintf("mkdir -p %s && "+
			"docker run -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block plan -p %s --max-time=%s | "+
			"docker run -v %s/:/shared -i quay.io/thanos/thanosbench:v0.3.0-rc.0 block gen --output.dir /shared", prom2Data, profile, maxTimeFresh, prom2Data),
	); err != nil {
		return err
	}
	return nil
}

// TestReadOnlyThanosSetup runs read only Thanos setup that has data from `maxTimeFresh - 2w` to `maxTimeOld`, with extra monitoring and tracing for full playground experience.
// Run with test args `-timeout 9999m`.
func TestReadOnlyThanosSetup(t *testing.T) {
	t.Skip("This is interactive test - it will run until you will kill it or curl 'finish' endpoint. Comment and run as normal test to use it!")

	// Create series of TSDB blocks. Cache them to 'data' dir so we don't need to re-create on every run.
	_, err := os.Stat(data)
	if os.IsNotExist(err) {
		testutil.Ok(t, createData())
	} else {
		testutil.Ok(t, err)
		fmt.Println("Skipping blocks generation, found data directory.")
	}

	e, err := e2e.NewDockerEnvironment("interactive")
	testutil.Ok(t, err)
	t.Cleanup(e.Close)

	p, err := e2eprof.Start(e)
	testutil.Ok(t, err)

	m, err := e2emon.Start(e)
	testutil.Ok(t, err)

	// Initialize object storage with two buckets (our long term storage).
	//
	//	┌──────────────┐
	//	│              │
	//	│    Minio     │
	//	│              │
	//	├──────────────┼──────────────────────────────────────────────────┐
	//	│ Bucket: bkt1 │ {cluster=eu1, replica=0} 10k series [t-2w, t-1w] │
	//	├──────────────┼──────────────────────────────────────────────────┘
	//	│              │
	//	├──────────────┼──────────────────────────────────────────────────┐
	//	│ Bucket: bkt2 │ {cluster=us1, replica=0} 10k series [t-2w, t-1w] │
	//	└──────────────┴──────────────────────────────────────────────────┘
	//
	m1 := e2edb.NewMinio(e, "minio-1", "default")
	testutil.Ok(t, exec("cp", "-r", store1Data+"/.", filepath.Join(m1.Dir(), "bkt1")))
	testutil.Ok(t, exec("cp", "-r", store2Data+"/.", filepath.Join(m1.Dir(), "bkt2")))

	// Setup Jaeger.
	j := e.Runnable("tracing").WithPorts(map[string]int{"http-front": 16686, "jaeger.thrift": 14268}).Init(e2e.StartOptions{Image: "jaegertracing/all-in-one:1.25"})
	testutil.Ok(t, e2e.StartAndWaitReady(j))

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

	// Create two store gateways, one for each bucket (access point to long term storage).
	//	                    ┌───────────┐
	//	                    │           │
	//	┌──────────────┐    │  Store 1  │
	//	│ Bucket: bkt1 │◄───┤           │
	//	├──────────────┼    └───────────┘
	//	│              │
	//	├──────────────┼    ┌───────────┐
	//	│ Bucket: bkt2 │◄───┤           │
	//	└──────────────┴    │  Store 2  │
	//	                    │           │
	//	                    └───────────┘
	bkt1Config, err := yaml.Marshal(client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    "bkt1",
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m1.InternalEndpoint("http"),
			Insecure:  true,
		},
	})
	testutil.Ok(t, err)
	store1 := e2edb.NewThanosStore(
		e,
		"store1",
		bkt1Config,
		e2edb.WithImage("thanos:latest"),
		e2edb.WithFlagOverride(map[string]string{
			"--tracing.config":    string(jaegerConfig),
			"--consistency-delay": "0s",
		}),
	)

	bkt2Config, err := yaml.Marshal(client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    "bkt2",
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m1.InternalEndpoint("http"),
			Insecure:  true,
		},
	})
	testutil.Ok(t, err)

	store2 := e2edb.NewThanosStore(
		e,
		"store2",
		bkt2Config,
		e2edb.WithImage("thanos:latest"),
		e2edb.WithFlagOverride(map[string]string{
			"--tracing.config":    string(jaegerConfig),
			"--consistency-delay": "0s",
		}),
	)

	// Create two Prometheus replicas in HA, and one separate one (short term storage + scraping).
	// Add a Thanos sidecar.
	//
	//                                                         ┌────────────┐
	//    ┌───────────────────────────────────────────────┐    │            │
	//    │ {cluster=eu1, replica=0} 10k series [t-1w, t] │◄───┤  Prom-ha0  │
	//    └───────────────────────────────────────────────┘    │            │
	//                                                         ├────────────┤
	//                                                         │   Sidecar  │
	//                                                         └────────────┘
	//
	//                                                         ┌────────────┐
	//    ┌───────────────────────────────────────────────┐    │            │
	//    │ {cluster=eu1, replica=1} 10k series [t-1w, t] │◄───┤  Prom-ha1  │
	//    └───────────────────────────────────────────────┘    │            │
	//                                                         ├────────────┤
	//                                                         │   Sidecar  │
	//                                                         └────────────┘
	//
	//                                                         ┌────────────┐
	//    ┌───────────────────────────────────────────────┐    │            │
	//    │ {cluster=us1, replica=0} 10k series [t-1w, t] │◄───┤  Prom 2    │
	//    └───────────────────────────────────────────────┘    │            │
	//                                                         ├────────────┤
	//                                                         │   Sidecar  │
	//                                                         └────────────┘
	promHA0 := e2edb.NewPrometheus(e, "prom-ha0")
	promHA1 := e2edb.NewPrometheus(e, "prom-ha1")
	prom2 := e2edb.NewPrometheus(e, "prom2")

	sidecarHA0 := e2edb.NewThanosSidecar(e, "sidecar-prom-ha0", promHA0, e2edb.WithImage("thanos:latest"), e2edb.WithFlagOverride(map[string]string{"--tracing.config": string(jaegerConfig)}))
	sidecarHA1 := e2edb.NewThanosSidecar(e, "sidecar-prom-ha1", promHA1, e2edb.WithImage("thanos:latest"), e2edb.WithFlagOverride(map[string]string{"--tracing.config": string(jaegerConfig)}))
	sidecar2 := e2edb.NewThanosSidecar(e, "sidecar2", prom2, e2edb.WithImage("thanos:latest"))

	receive1 := e2ethanos.NewReceiveBuilder(e, "receiver-1").WithIngestionEnabled().Init()

	testutil.Ok(t, exec("cp", "-r", prom1Data+"/.", promHA0.Dir()))
	testutil.Ok(t, exec("sh", "-c", "find "+prom1Data+"/ -maxdepth 1 -type d | tail -5 | xargs -I {} cp -r {} "+promHA1.Dir())) // Copy only 5 blocks from 9 to mimic replica 1 with partial data set.
	testutil.Ok(t, exec("cp", "-r", prom2Data+"/.", prom2.Dir()))

	testutil.Ok(t, promHA0.SetConfig(promconfig.Config{
		GlobalConfig: promconfig.GlobalConfig{
			ExternalLabels: map[model.LabelName]model.LabelValue{
				"cluster": "eu-1",
				"replica": "0",
			},
		},
	}))
	testutil.Ok(t, promHA1.SetConfig(promconfig.Config{
		GlobalConfig: promconfig.GlobalConfig{
			ExternalLabels: map[model.LabelName]model.LabelValue{
				"cluster": "eu-1",
				"replica": "1",
			},
		},
	}))
	testutil.Ok(t, prom2.SetConfig(promconfig.Config{
		GlobalConfig: promconfig.GlobalConfig{
			ExternalLabels: map[model.LabelName]model.LabelValue{
				"cluster": "us-1",
				"replica": "0",
			},
		},
	}))

	testutil.Ok(t, e2e.StartAndWaitReady(m1))
	testutil.Ok(t, e2e.StartAndWaitReady(promHA0, promHA1, prom2, sidecarHA0, sidecarHA1, sidecar2, store1, store2, receive1))

	// Let's start query on top of all those 6 store APIs (global query engine).
	//
	//  ┌──────────────┐
	//  │              │
	//  │    Minio     │                                                       ┌───────────┐
	//  │              │                                                       │           │
	//  ├──────────────┼──────────────────────────────────────────────────┐    │  Store 1  │◄──────┐
	//  │ Bucket: bkt1 │ {cluster=eu1, replica=0} 10k series [t-2w, t-1w] │◄───┤           │       │
	//  ├──────────────┼──────────────────────────────────────────────────┘    └───────────┘       │
	//  │              │                                                                           │
	//  ├──────────────┼──────────────────────────────────────────────────┐    ┌───────────┐       │
	//  │ Bucket: bkt2 │ {cluster=us1, replica=0} 10k series [t-2w, t-1w] │◄───┤           │       │
	//  └──────────────┴──────────────────────────────────────────────────┘    │  Store 2  │◄──────┤
	//                                                                         │           │       │
	//                                                                         └───────────┘       │
	//                                                                                             │
	//                                                                                             │
	//                                                                                             │      ┌───────────────┐
	//                                                                         ┌────────────┐      │      │               │
	//                    ┌───────────────────────────────────────────────┐    │            │      ├──────┤    Querier    │◄────── PromQL
	//                    │ {cluster=eu1, replica=0} 10k series [t-1w, t] │◄───┤  Prom-ha0  │      │      │               │
	//                    └───────────────────────────────────────────────┘    │            │      │      └───────────────┘
	//                                                                         ├────────────┤      │
	//                                                                         │   Sidecar  │◄─────┤
	//                                                                         └────────────┘      │
	//                                                                                             │
	//                                                                         ┌────────────┐      │
	//                    ┌───────────────────────────────────────────────┐    │            │      │
	//                    │ {cluster=eu1, replica=1} 10k series [t-1w, t] │◄───┤  Prom-ha1  │      │
	//                    └───────────────────────────────────────────────┘    │            │      │
	//                                                                         ├────────────┤      │
	//                                                                         │   Sidecar  │◄─────┤
	//                                                                         └────────────┘      │
	//                                                                                             │
	//                                                                         ┌────────────┐      │
	//                    ┌───────────────────────────────────────────────┐    │            │      │
	//                    │ {cluster=us1, replica=0} 10k series [t-1w, t] │◄───┤  Prom 2    │      │
	//                    └───────────────────────────────────────────────┘    │            │      │
	//                                                                         ├────────────┤      │
	//                                                                         │   Sidecar  │◄─────┘
	//                                                                         └────────────┘
	//
	query1 := e2edb.NewThanosQuerier(
		e,
		"query1",
		[]string{
			store1.InternalEndpoint("grpc"),
			store2.InternalEndpoint("grpc"),
			sidecarHA0.InternalEndpoint("grpc"),
			sidecarHA1.InternalEndpoint("grpc"),
			sidecar2.InternalEndpoint("grpc"),
			receive1.InternalEndpoint("grpc"),
		},
		e2edb.WithImage("thanos:latest"),
		e2edb.WithFlagOverride(map[string]string{"--tracing.config": string(jaegerConfig)}),
	)
	testutil.Ok(t, e2e.StartAndWaitReady(query1))

	// Wait until we have 6 gRPC connections.
	testutil.Ok(t, query1.WaitSumMetricsWithOptions(e2emon.Equals(6), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	const path = "graph?g0.expr=sum(continuous_app_metric0)%20by%20(cluster%2C%20replica)&g0.tab=0&g0.stacked=0&g0.range_input=2w&g0.max_source_resolution=0s&g0.deduplicate=0&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2021-07-27%2000%3A00%3A00"
	testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", query1.Endpoint("http"), path)))
	testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", prom2.Endpoint("http"), path)))

	// Tracing endpoint.
	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+j.Endpoint("http-front")))
	// Profiling Endpoint.
	testutil.Ok(t, p.OpenUserInterfaceInBrowser())
	// Monitoring Endpoint.
	testutil.Ok(t, m.OpenUserInterfaceInBrowser())
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
}
