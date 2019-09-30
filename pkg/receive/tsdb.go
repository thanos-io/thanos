package receive

import (
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/tsdb"
	promtsdb "github.com/prometheus/prometheus/tsdb"
)

type FlushableStorage struct {
	*promtsdb.DB

	path string
	l    log.Logger
	r    prometheus.Registerer
	opts *tsdb.Options

	stopped bool
	mu      sync.Mutex
}

// NewFlushableStorage returns a new storage backed by a TSDB database that is configured for Prometheus.
func NewFlushableStorage(path string, l log.Logger, r prometheus.Registerer, opts *tsdb.Options) *FlushableStorage {
	return &FlushableStorage{
		path:    path,
		l:       l,
		r:       r,
		opts:    opts,
		stopped: true,
	}
}

// Get returns a reference to the underlying storage.
func (f *FlushableStorage) Get() *promtsdb.DB {
	return f.DB
}

// Open starts the TSDB.
func (f *FlushableStorage) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open()
}

// open starts the TSDB.
// This is only meant to be used internally as it is not
// concurrency-safe on its own.
func (f *FlushableStorage) open() error {
	if !f.stopped {
		return nil
	}
	db, err := tsdb.Open(
		f.path,
		log.With(f.l, "component", "tsdb"),
		&UnRegisterer{f.r},
		f.opts,
	)
	if err != nil {
		return err
	}
	f.DB = db
	f.stopped = false
	return nil
}

// Flush temporarily stops the storage and flushes the WAL to blocks.
// Note: this operation leaves the storage in the same state it was in.
func (f *FlushableStorage) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var reopen bool
	if !f.stopped {
		if err := f.DB.Close(); err != nil {
			return errors.Wrap(err, "stopping storage")
		}
		f.stopped = true
		reopen = true
	}
	ro, err := promtsdb.OpenDBReadOnly(f.Dir(), f.l)
	if err != nil {
		return errors.Wrap(err, "opening read-only DB")
	}
	if err := ro.FlushWAL(f.Dir()); err != nil {
		return errors.Wrap(err, "flushing WAL")
	}
	if reopen {
		return errors.Wrap(f.open(), "re-starting storage")
	}
	return nil
}

// Close stops the storage.
func (f *FlushableStorage) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return nil
	}
	if err := f.DB.Close(); err != nil {
		return err
	}
	f.stopped = true
	return nil
}

// UnRegisterer is a Prometheus registerer that
// ensures that collectors can be registered
// by unregistering already-registered collectors.
// FlushableStorage uses this registerer in order
// to not lose metric values between DB flushes.
type UnRegisterer struct {
	prometheus.Registerer
}

func (u *UnRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := u.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if ok = u.Unregister(c); !ok {
					panic("unable to unregister existing collector")
				}
				u.Registerer.MustRegister(c)
				continue
			}
			panic(err)
		}
	}
}
