// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querypb

import (
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/logicalplan"
)

func NewJSONEncodedPlan(plan api.RemoteQuery) (*QueryPlan, error) {
	node, ok := plan.(logicalplan.Node)
	if !ok {
		return nil, nil
	}
	bytes, err := logicalplan.Marshal(node)
	if err != nil {
		return nil, err
	}

	return &QueryPlan{
		Encoding: &QueryPlan_Json{Json: bytes},
	}, nil
}
