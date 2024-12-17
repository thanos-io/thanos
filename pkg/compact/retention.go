// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	tenantRetentionRegex = `^([\w-]+):((\d{4}-\d{2}-\d{2})|(\d+d))$`
)

// ApplyRetentionPolicyByResolution removes blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func ApplyRetentionPolicyByResolution(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	retentionByResolution map[ResolutionLevel]time.Duration,
	blocksMarkedForDeletion prometheus.Counter,
) error {
	level.Info(logger).Log("msg", "start optional retention")
	for id, m := range metas {
		retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
		if retentionDuration.Seconds() == 0 {
			continue
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(retentionDuration)) {
			level.Info(logger).Log("msg", "applying retention: marking block for deletion", "id", id, "maxTime", maxTime.String())
			if err := block.MarkForDeletion(ctx, logger, bkt, id, fmt.Sprintf("block exceeding retention of %v", retentionDuration), blocksMarkedForDeletion); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}
	}
	level.Info(logger).Log("msg", "optional retention apply done")
	return nil
}

type RetentionPolicy struct {
	CutoffDate        time.Time
	RetentionDuration time.Duration
}

func (r RetentionPolicy) isExpired(blockMaxTime time.Time) bool {
	if r.CutoffDate.IsZero() {
		return time.Now().After(blockMaxTime.Add(r.RetentionDuration))
	}
	return r.CutoffDate.After(blockMaxTime)
}

func ParesRetentionPolicyByTenant(logger log.Logger, retentionTenants []string) (map[string]RetentionPolicy, error) {
	pattern := regexp.MustCompile(tenantRetentionRegex)
	retentionByTenant := make(map[string]RetentionPolicy, len(retentionTenants))
	for _, tenantRetention := range retentionTenants {
		matches := pattern.FindStringSubmatch(tenantRetention)
		invalidFormat := errors.Errorf("invalid retention format for tenant: %s, must be `<tenant>:(<yyyy-mm-dd>|<duration>d)`", tenantRetention)
		if len(matches) != 5 {
			return nil, errors.Wrapf(invalidFormat, "matched size %d", len(matches))
		}
		tenant := matches[1]
		var policy RetentionPolicy
		if _, ok := retentionByTenant[tenant]; ok {
			return nil, errors.Errorf("duplicate retention policy for tenant: %s", tenant)
		}
		if cutoffDate, err := time.Parse(time.DateOnly, matches[3]); err != nil && matches[3] != "" {
			return nil, errors.Wrapf(invalidFormat, "error parsing cutoff date: %v", err)
		} else if matches[3] != "" {
			policy.CutoffDate = cutoffDate
		}
		if duration, err := model.ParseDuration(matches[4]); err != nil && matches[4] != "" {
			return nil, errors.Wrapf(invalidFormat, "error parsing duration: %v", err)
		} else if matches[4] != "" {
			policy.RetentionDuration = time.Duration(duration)
		}
		level.Info(logger).Log("msg", "retention policy for tenant is enabled", "tenant", tenant, "retention policy", fmt.Sprintf("%v", policy))
		retentionByTenant[tenant] = policy
	}
	return retentionByTenant, nil
}

// ApplyRetentionPolicyByTenant removes blocks depending on the specified retentionByTenant based on blocks MaxTime.
func ApplyRetentionPolicyByTenant(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	retentionByTenant map[string]RetentionPolicy,
	blocksMarkedForDeletion prometheus.Counter) error {
	if len(retentionByTenant) == 0 {
		level.Info(logger).Log("msg", "tenant retention is disabled due to no policy")
		return nil
	}
	level.Info(logger).Log("msg", "start tenant retention")
	for id, m := range metas {
		policy, ok := retentionByTenant[m.Thanos.GetTenant()]
		if !ok {
			continue
		}
		maxTime := time.Unix(m.MaxTime/1000, 0)
		if policy.isExpired(maxTime) {
			level.Info(logger).Log("msg", "applying retention: marking block for deletion", "id", id, "maxTime", maxTime.String())
			if err := block.MarkForDeletion(ctx, logger, bkt, id, fmt.Sprintf("block exceeding retention of %v", policy), blocksMarkedForDeletion); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}
	}
	level.Info(logger).Log("msg", "tenant retention apply done")
	return nil
}
