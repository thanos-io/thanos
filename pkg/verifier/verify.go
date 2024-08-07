// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/block"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/objstore"
)

type Verifier interface {
	IssueID() string
	Verify(ctx Context, idMatcher func(ulid.ULID) bool) error
}

type VerifierRepairer interface {
	IssueID() string
	VerifyRepair(ctx Context, idMatcher func(ulid.ULID) bool, repair bool) error
}

// Context is an verifier config.
type Context struct {
	context.Context

	Logger      log.Logger
	Bkt         objstore.Bucket
	BackupBkt   objstore.Bucket
	Fetcher     block.MetadataFetcher
	DeleteDelay time.Duration

	metrics *metrics
}

type metrics struct {
	blocksMarkedForDeletion prometheus.Counter
}

func newVerifierMetrics(reg prometheus.Registerer) *metrics {
	var m metrics
	m.blocksMarkedForDeletion = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_verify_blocks_marked_for_deletion_total",
		Help: "Total number of blocks marked for deletion by verify.",
	})
	return &m
}

// Manager runs given issues to verify if bucket is healthy.
type Manager struct {
	Context

	vs Registry
}

type Registry struct {
	Verifiers         []Verifier
	VerifierRepairers []VerifierRepairer
}

func (r Registry) VerifiersIDs() []string {
	ks := make([]string, 0, len(r.Verifiers))
	for _, v := range r.Verifiers {
		ks = append(ks, v.IssueID())
	}
	return ks
}

func (r Registry) VerifierRepairersIDs() []string {
	ks := make([]string, 0, len(r.VerifierRepairers))
	for _, vr := range r.VerifierRepairers {
		ks = append(ks, vr.IssueID())
	}
	return ks
}

func (r Registry) SubstractByIDs(ids []string, repair bool) (Registry, error) {
	n := Registry{}
idLoop:
	for _, id := range ids {
		if !repair {
			for _, v := range r.Verifiers {
				if v.IssueID() != id {
					continue
				}
				n.Verifiers = append(n.Verifiers, v)
				continue idLoop

			}
		}

		for _, vr := range r.VerifierRepairers {
			if vr.IssueID() != id {
				continue
			}
			n.VerifierRepairers = append(n.VerifierRepairers, vr)
			continue idLoop
		}
		return n, errors.Errorf("no such issue ID %s", id)
	}
	return n, nil
}

// New returns verifier's manager.
func NewManager(reg prometheus.Registerer, logger log.Logger, bkt, backupBkt objstore.Bucket, fetcher block.MetadataFetcher, deleteDelay time.Duration, vs Registry) *Manager {
	return &Manager{
		Context: Context{
			Logger:      logger,
			Bkt:         bkt,
			BackupBkt:   backupBkt,
			Fetcher:     fetcher,
			DeleteDelay: deleteDelay,

			metrics: newVerifierMetrics(reg),
		},
		vs: vs,
	}
}

// Verify verifies matching blocks using registered list of Verifier and VerifierRepairer.
// TODO(blotka): Wrap bucket with WrapWithMetrics and print metrics after each issue (e.g how many blocks where touched).
func (m *Manager) Verify(ctx context.Context, idMatcher func(ulid.ULID) bool) error {
	if len(m.vs.Verifiers)+len(m.vs.VerifierRepairers) == 0 {
		return errors.New("nothing to verify. No verifiers and verifierRepairers registered")
	}

	logger := log.With(m.Logger, "verifiers", strings.Join(append(m.vs.VerifiersIDs(), m.vs.VerifierRepairersIDs()...), ","))
	level.Info(logger).Log("msg", "Starting verify task")

	for _, v := range m.vs.Verifiers {
		vCtx := m.Context
		vCtx.Logger = log.With(logger, "verifier", v.IssueID())
		vCtx.Context = ctx
		if err := v.Verify(vCtx, idMatcher); err != nil {
			return errors.Wrapf(err, "verify %s", v.IssueID())
		}
	}
	for _, vr := range m.vs.VerifierRepairers {
		vCtx := m.Context
		vCtx.Context = ctx
		vCtx.Logger = log.With(logger, "verifier", vr.IssueID())
		if err := vr.VerifyRepair(vCtx, idMatcher, false); err != nil {
			return errors.Wrapf(err, "verify %s", vr.IssueID())
		}
	}

	level.Info(logger).Log("msg", "verify task completed")
	return nil
}

// VerifyAndRepair verifies and repairs matching blocks using registered list of VerifierRepairer.
// TODO(blotka): Wrap bucket with WrapWithMetrics and print metrics after each issue (e.g how many blocks where touched).
func (m *Manager) VerifyAndRepair(ctx context.Context, idMatcher func(ulid.ULID) bool) error {
	if len(m.vs.Verifiers)+len(m.vs.VerifierRepairers) == 0 {
		return errors.New("nothing to verify. No verifierRepairers registered")
	}

	logger := log.With(m.Logger, "verifiers", strings.Join(m.vs.VerifierRepairersIDs(), ","))
	level.Warn(logger).Log("msg", "GLOBAL COMPACTOR SHOULD __NOT__ BE RUNNING ON THE SAME BUCKET")
	level.Info(logger).Log("msg", "Starting verify and repair task")

	for _, vr := range m.vs.VerifierRepairers {
		vCtx := m.Context
		vCtx.Logger = log.With(logger, "verifier", vr.IssueID())
		vCtx.Context = ctx
		if err := vr.VerifyRepair(vCtx, idMatcher, true); err != nil {
			return errors.Wrapf(err, "verify and repair %s", vr.IssueID())
		}
	}

	level.Info(logger).Log("msg", "verify and repair task completed")
	return nil
}
