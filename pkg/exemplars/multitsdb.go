// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"fmt"

	"github.com/pkg/errors"

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
	fmt.Println("meer paas request aayi", m.tsdbExemplarsServers())
	for tenant, es := range m.tsdbExemplarsServers() {
		if err := es.Exemplars(r, s); err != nil {
			return errors.Wrapf(err, "get info for tenant %s", tenant)
		}
	}
	return nil
}
