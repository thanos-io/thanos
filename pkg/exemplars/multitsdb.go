// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
)

// MultiTSDB implements exemplarspb.ExemplarsServer that allows to fetch exemplars a MultiTSDB instance.
type MultiTSDB struct {
	tsdbExemplarsServers func() map[string]exemplarspb.ExemplarsServer
}

// NewMultiTSDB creates new exemplars.MultiTSDB.
func NewMultiTSDB(tsdbExemplarsServers func() map[string]exemplarspb.ExemplarsServer) *MultiTSDB {
	return &MultiTSDB{
		tsdbExemplarsServers: tsdbExemplarsServers,
	}
}

// Exemplars returns all specified exemplars from a MultiTSDB instance.
func (m *MultiTSDB) Exemplars(r *exemplarspb.ExemplarsRequest, s exemplarspb.Exemplars_ExemplarsServer) error {
	for tenant, es := range m.tsdbExemplarsServers() {
		if err := es.Exemplars(r, s); err != nil {
			return status.Error(codes.Aborted, errors.Wrapf(err, "get exemplars for tenant %s", tenant).Error())
		}
	}
	return nil
}
