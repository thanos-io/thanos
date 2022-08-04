// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package kv

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/internal/cortex/ring/kv/codec"
)

func TestParseConfig(t *testing.T) {
	conf := `
store: consul
consul:
  host: "consul:8500"
  consistentreads: true
prefix: "test/"
multi:
  primary: consul
  secondary: etcd
`

	cfg := Config{}

	err := yaml.Unmarshal([]byte(conf), &cfg)
	require.NoError(t, err)
	require.Equal(t, "consul", cfg.Store)
	require.Equal(t, "test/", cfg.Prefix)
	require.Equal(t, "consul:8500", cfg.Consul.Host)
	require.Equal(t, "consul", cfg.Multi.Primary)
	require.Equal(t, "etcd", cfg.Multi.Secondary)
}

func Test_createClient_multiBackend_withSingleRing(t *testing.T) {
	storeCfg, testCodec := newConfigsForTest()
	require.NotPanics(t, func() {
		_, err := createClient("multi", "/collector", storeCfg, testCodec, Primary, prometheus.NewPedanticRegistry(), testLogger{})
		require.NoError(t, err)
	})
}

func Test_createClient_multiBackend_withMultiRing(t *testing.T) {
	storeCfg1, testCodec := newConfigsForTest()
	storeCfg2 := StoreConfig{}
	reg := prometheus.NewPedanticRegistry()

	require.NotPanics(t, func() {
		_, err := createClient("multi", "/test", storeCfg1, testCodec, Primary, reg, testLogger{})
		require.NoError(t, err)
	}, "First client for KV store must not panic")
	require.NotPanics(t, func() {
		_, err := createClient("mock", "/test", storeCfg2, testCodec, Primary, reg, testLogger{})
		require.NoError(t, err)
	}, "Second client for KV store must not panic")
}

func Test_createClient_singleBackend_mustContainRoleAndTypeLabels(t *testing.T) {
	storeCfg, testCodec := newConfigsForTest()
	reg := prometheus.NewPedanticRegistry()
	client, err := createClient("mock", "/test1", storeCfg, testCodec, Primary, reg, testLogger{})
	require.NoError(t, err)
	require.NoError(t, client.CAS(context.Background(), "/test", func(_ interface{}) (out interface{}, retry bool, err error) {
		out = &mockMessage{id: "inCAS"}
		retry = false
		return
	}))

	actual := typeToRoleMapHistogramLabels(t, reg, "kv_request_duration_seconds")
	require.Len(t, actual, 1)
	require.Equal(t, "primary", actual["mock"])
}

func Test_createClient_multiBackend_mustContainRoleAndTypeLabels(t *testing.T) {
	storeCfg, testCodec := newConfigsForTest()
	storeCfg.Multi.MirrorEnabled = true
	storeCfg.Multi.MirrorTimeout = 10 * time.Second
	reg := prometheus.NewPedanticRegistry()
	client, err := createClient("multi", "/test1", storeCfg, testCodec, Primary, reg, testLogger{})
	require.NoError(t, err)
	require.NoError(t, client.CAS(context.Background(), "/test", func(_ interface{}) (out interface{}, retry bool, err error) {
		out = &mockMessage{id: "inCAS"}
		retry = false
		return
	}))

	actual := typeToRoleMapHistogramLabels(t, reg, "kv_request_duration_seconds")
	// expected multi-primary, inmemory-primary and mock-secondary
	require.Len(t, actual, 3)
	require.Equal(t, "primary", actual["multi"])
	require.Equal(t, "primary", actual["inmemory"])
	require.Equal(t, "secondary", actual["mock"])

}

func typeToRoleMapHistogramLabels(t *testing.T, reg prometheus.Gatherer, histogramWithRoleLabels string) map[string]string {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	result := map[string]string{}
	for _, mf := range mfs {
		if mf.GetName() != histogramWithRoleLabels {
			continue
		}
		for _, m := range mf.GetMetric() {
			backendType := ""
			role := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "role" {
					role = l.GetValue()
				} else if l.GetName() == "type" {
					backendType = l.GetValue()
				}
			}
			require.NotEmpty(t, backendType)
			require.NotEmpty(t, role)
			result[backendType] = role
		}
	}
	return result
}
func newConfigsForTest() (cfg StoreConfig, c codec.Codec) {
	cfg = StoreConfig{
		Multi: MultiConfig{
			Primary:   "inmemory",
			Secondary: "mock",
		},
	}
	c = codec.NewProtoCodec("test", func() proto.Message {
		return &mockMessage{id: "inCodec"}
	})
	return
}

type mockMessage struct {
	id string
}

func (m *mockMessage) Reset() {
	panic("do not use")
}

func (m *mockMessage) String() string {
	panic("do not use")
}

func (m *mockMessage) ProtoMessage() {
	panic("do not use")
}

type testLogger struct {
}

func (l testLogger) Log(keyvals ...interface{}) error {
	return nil
}
