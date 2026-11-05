// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"

	"github.com/efficientgo/core/testutil"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  any
		err  error
	}{
		{
			name: "<nil> config",
			cfg:  nil,
			err:  errEmptyConfigurationFile,
		},
		{
			name: "empty config",
			cfg:  []HashringConfig{},
			err:  errEmptyConfigurationFile,
		},
		{
			name: "unparsable config",
			cfg:  struct{}{},
			err:  errParseConfigurationFile,
		},
		{
			name: "valid config",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			err: nil, // means it's valid.
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content, err := json.Marshal(tc.cfg)
			testutil.Ok(t, err)

			tmpfile, err := os.CreateTemp("", "configwatcher_test.*.json")
			testutil.Ok(t, err)

			defer func() {
				testutil.Ok(t, os.Remove(tmpfile.Name()))
			}()

			_, err = tmpfile.Write(content)
			testutil.Ok(t, err)

			err = tmpfile.Close()
			testutil.Ok(t, err)

			cw, err := NewConfigWatcher(nil, nil, tmpfile.Name(), 1)
			testutil.Ok(t, err)
			defer cw.Stop()

			if err := cw.ValidateConfig(); err != nil && !errors.Is(err, tc.err) {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
			}
		})
	}
}

// TestConfigWatcher_ChangesCounterNotDoubleIncremented guards against a
// regression where both the underlying filewatch primitive and the wrapper's
// refresh() bumped the same changesCounter, producing 2x the expected value
// per detected change (and also bumping on parse failures, which the
// pre-refactor metric never did). After the fix, the primitive's
// ChangesCounter injection is nil and the wrapper is the sole owner.
func TestConfigWatcher_ChangesCounterNotDoubleIncremented(t *testing.T) {
	t.Parallel()

	initial := []HashringConfig{{Hashring: "h1", Endpoints: []Endpoint{{Address: "node1"}}}}
	updated := []HashringConfig{{Hashring: "h1", Endpoints: []Endpoint{{Address: "node2"}}}}

	dir := t.TempDir()
	path := filepath.Join(dir, "hashring.json")
	writeJSON := func(v any) {
		t.Helper()
		b, err := json.Marshal(v)
		testutil.Ok(t, err)
		testutil.Ok(t, os.WriteFile(path, b, 0644))
	}
	writeJSON(initial)

	reg := prometheus.NewRegistry()
	cw, err := NewConfigWatcher(nil, reg, path, model.Duration(200*time.Millisecond))
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		cw.Run(ctx)
		close(runDone)
	}()

	// Drain the initial emit.
	select {
	case <-cw.C():
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive initial emit")
	}

	// One file change. We expect exactly one increment of changesCounter.
	writeJSON(updated)
	select {
	case <-cw.C():
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive updated emit")
	}

	// Allow any spurious secondary ticks to land before sampling.
	time.Sleep(300 * time.Millisecond)
	got := promtestutil.ToFloat64(cw.changesCounter)
	// 1 for the initial load + 1 for the single update == 2.
	// (Previously this would have been 3+ due to the primitive bumping in
	// addition to the wrapper.)
	testutil.Equals(t, float64(2), got, "expected changesCounter == 2 (initial + one update)")

	cancel()
	<-runDone
}

// TestConfigWatcher_StopIsIdempotent asserts that Stop can be called more
// than once without panicking. This guards against accidentally closing the
// already-closed update channel; callers (e.g. cmd/thanos/receive.go when
// ValidateConfig fails, and Run's defer) may both invoke Stop.
func TestConfigWatcher_StopIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "hashring.json")
	testutil.Ok(t, os.WriteFile(path, []byte(`[{"endpoints":[{"address":"node1"}]}]`), 0644))

	cw, err := NewConfigWatcher(nil, nil, path, model.Duration(10*time.Second))
	testutil.Ok(t, err)

	cw.Stop()
	cw.Stop() // would panic on close-of-closed-channel without the sync.Once guard.
}

// TestConfigWatcher_EmitsOnInitialAndChange asserts the end-to-end refactor:
// the wrapper performs the initial parse on Run start, then reacts to the
// underlying primitive's signal by re-parsing and re-emitting on the typed
// channel. This covers the wiring that no other existing test exercises
// (TestValidateConfig only checks the construction-time parse).
func TestConfigWatcher_EmitsOnInitialAndChange(t *testing.T) {
	t.Parallel()

	// UnmarshalJSON auto-fills CapNProtoAddress with the default port when
	// not set, so the expected values mirror what the deserializer produces.
	initial := []HashringConfig{{
		Hashring:  "h1",
		Endpoints: []Endpoint{{Address: "node1", CapNProtoAddress: "node1:" + DefaultCapNProtoPort}},
	}}
	updated := []HashringConfig{{
		Hashring: "h1",
		Endpoints: []Endpoint{
			{Address: "node1", CapNProtoAddress: "node1:" + DefaultCapNProtoPort},
			{Address: "node2", CapNProtoAddress: "node2:" + DefaultCapNProtoPort},
		},
	}}
	// Inputs use plain address strings (UnmarshalJSON handles either form).
	initialInput := []HashringConfig{{
		Hashring:  "h1",
		Endpoints: []Endpoint{{Address: "node1"}},
	}}
	updatedInput := []HashringConfig{{
		Hashring:  "h1",
		Endpoints: []Endpoint{{Address: "node1"}, {Address: "node2"}},
	}}

	dir := t.TempDir()
	path := filepath.Join(dir, "hashring.json")
	writeJSON := func(v any) {
		t.Helper()
		b, err := json.Marshal(v)
		testutil.Ok(t, err)
		testutil.Ok(t, os.WriteFile(path, b, 0644))
	}
	writeJSON(initialInput)

	// Short interval so the safety-net tick catches the change even if the
	// fsnotify event somehow slips through (e.g. on a slow CI runner).
	cw, err := NewConfigWatcher(nil, nil, path, model.Duration(200*time.Millisecond))
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		cw.Run(ctx)
		close(runDone)
	}()

	// Initial config must be emitted without any file mutation.
	select {
	case got, ok := <-cw.C():
		testutil.Equals(t, true, ok, "channel should be open")
		testutil.Equals(t, initial, got, "initial config should be emitted on Run start")
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive initial config within 3s")
	}

	// Update the file; the watcher should detect and re-emit.
	writeJSON(updatedInput)

	select {
	case got, ok := <-cw.C():
		testutil.Equals(t, true, ok, "channel should be open")
		testutil.Equals(t, updated, got, "updated config should be emitted after file change")
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive updated config within 3s")
	}

	cancel()
	<-runDone
}

func TestUnmarshalEndpointSlice(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		json      string
		endpoints []Endpoint
		expectErr bool
	}{
		{
			name:      "Endpoint with empty address",
			json:      `[{"az": "az-1"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391"}},
			expectErr: true,
		},
		{
			name:      "Endpoints as string slice",
			json:      `["node-1"]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391"}},
		},
		{
			name:      "Endpoints as endpoints slice",
			json:      `[{"address": "node-1", "az": "az-1"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391", AZ: "az-1"}},
		},
		{
			name:      "Endpoints as string slice with port",
			json:      `["node-1:80"]`,
			endpoints: []Endpoint{{Address: "node-1:80", CapNProtoAddress: "node-1:19391"}},
		},
		{
			name:      "Endpoints as string slice with capnproto port",
			json:      `[{"address": "node-1", "capnproto_address": "node-1:81"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:81"}},
		},
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			var endpoints []Endpoint
			err := json.Unmarshal([]byte(tcase.json), &endpoints)
			if tcase.expectErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.endpoints, endpoints)
		})
	}
}
