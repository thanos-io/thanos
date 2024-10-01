// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

type safeClients struct {
	sync.RWMutex
	clients []store.Client
}

func (sc *safeClients) get() []store.Client {
	sc.RLock()
	defer sc.RUnlock()
	ret := make([]store.Client, len(sc.clients))
	copy(ret, sc.clients)
	return ret
}

func (sc *safeClients) reset() {
	sc.Lock()
	defer sc.Unlock()
	sc.clients = sc.clients[:0]
}

func (sc *safeClients) append(c store.Client) {
	sc.Lock()
	defer sc.Unlock()
	sc.clients = append(sc.clients, c)
}

func TestQuerier_Proxy(t *testing.T) {
	files, err := filepath.Glob("testdata/promql/**/*.test")
	testutil.Ok(t, err)
	testutil.Equals(t, 10, len(files), "%v", files)

	logger := log.NewLogfmtLogger(os.Stderr)
	t.Run("proxy", func(t *testing.T) {
		var sc safeClients
		q := NewQueryableCreator(
			logger,
			nil,
			store.NewProxyStore(logger, nil, func() []store.Client { return sc.get() },
				component.Debug, nil, 5*time.Minute, store.EagerRetrieval),
			1000000,
			5*time.Minute,
		)

		createQueryableFn := func(stores []*testStore) storage.Queryable {
			sc.reset()
			for i, st := range stores {
				m, err := storepb.PromMatchersToMatchers(st.matchers...)
				testutil.Ok(t, err)
				// TODO(bwplotka): Parse external labels.
				sc.append(&storetestutil.TestClient{
					Name:        fmt.Sprintf("store number %v", i),
					StoreClient: storepb.ServerAsClient(selectedStore(store.NewTSDBStore(logger, st.storage.DB, component.Debug, nil), m, st.mint, st.maxt)),
					MinTime:     st.mint,
					MaxTime:     st.maxt,
				})
			}
			return q(true,
				nil,
				nil,
				0,
				false,
				false,
				nil,
				NoopSeriesStatsReporter,
			)
		}

		for _, fn := range files {
			t.Run(fn, func(t *testing.T) {
				te, err := newTestFromFile(t, fn)
				testutil.Ok(t, err)
				testutil.Ok(t, te.run(createQueryableFn))
				te.close()
			})
		}
	})
}

// selectStore allows wrapping another storeEndpoints with additional time and matcher selection.
type selectStore struct {
	matchers []storepb.LabelMatcher

	storepb.StoreServer
	mint, maxt int64
}

// selectedStore wraps given store with selectStore.
func selectedStore(wrapped storepb.StoreServer, matchers []storepb.LabelMatcher, mint, maxt int64) *selectStore {
	return &selectStore{
		StoreServer: wrapped,
		matchers:    matchers,
		mint:        mint,
		maxt:        maxt,
	}
}

func (s *selectStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	if r.MinTime < s.mint {
		r.MinTime = s.mint
	}
	if r.MaxTime > s.maxt {
		r.MaxTime = s.maxt
	}

	matchers := make([]storepb.LabelMatcher, 0, len(r.Matchers))
	matchers = append(matchers, r.Matchers...)

	req := *r
	req.Matchers = matchers
	return s.StoreServer.Series(&req, srv)
}
