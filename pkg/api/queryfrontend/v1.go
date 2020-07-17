// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/weaveworks/common/user"
)

type API struct {
	logger log.Logger
}

func NewAPI(
	logger log.Logger,
) *API {
	return &API{
		logger: logger,
	}
}

// Register the API's endpoints in the given router.
// TODO: add tracer and logger. Instrument handlers.
func (api *API) Register(r *route.Router, handleFunc http.HandlerFunc) {
	instr := func(_ string, f http.HandlerFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Cortex frontend middlewares require orgID.
			f.ServeHTTP(w, r.WithContext(user.InjectOrgID(r.Context(), "fake")))
		})
		return hf
	}

	r.Get("/query", instr("query", handleFunc))
	r.Post("/query", instr("query", handleFunc))

	r.Get("/query_range", instr("query_range", handleFunc))
	r.Post("/query_range", instr("query_range", handleFunc))

	r.Get("/label/:name/values", instr("/label/:name/values", handleFunc))

	r.Get("/series", instr("series", handleFunc))
	r.Post("/series", instr("series", handleFunc))

	r.Get("/labels", instr("labels", handleFunc))
	r.Post("/labels", instr("labels", handleFunc))
}
