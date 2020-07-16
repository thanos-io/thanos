// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// API is a very simple API used by Thanos Compactor.
type API struct {
	logger log.Logger
}

// NewAPI creates an Thanos Compactor API.
func NewAPI(
	logger log.Logger,
) *API {
	return &API{
		logger: logger,
	}
}

func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware) {
	instr := func(name string, f qapi.ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			qapi.SetCORS(w)
			if data, warnings, err := f(r); err != nil {
				qapi.RespondError(w, err, data)
			} else if data != nil {
				qapi.Respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return ins.NewHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Post("/delete_series", instr("delete_series", api.deleteSeries))
}

func (api *API) deleteSeries(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	return "Delete series is not implemented yet.", nil, nil
}
