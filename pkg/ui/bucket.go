// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"github.com/go-kit/log"
	"github.com/prometheus/common/route"

	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI

	externalPrefix, prefixHeader string
	Err                          error
}

func NewBucketUI(logger log.Logger, externalPrefix, prefixHeader string, comp component.Component) *Bucket {
	tmplVariables := map[string]string{
		"Component": comp.String(),
	}

	tmplFuncs := queryTmplFuncs()

	return &Bucket{
		BaseUI:         NewBaseUI(log.With(logger, "component", "bucketUI"), tmplFuncs, tmplVariables, externalPrefix, prefixHeader, comp),
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	registerReactApp(r, ins, b.BaseUI)
}
