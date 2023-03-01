// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type emptyLabelValues struct {
	storepb.StoreServer
}

func NewEmptyLabelValuesStore(storeServer storepb.StoreServer) storepb.StoreServer {
	return &emptyLabelValues{StoreServer: storeServer}
}

func (n *emptyLabelValues) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{}, nil
}

func (n *emptyLabelValues) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}
