// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This package is a modified copy from
// github.com/prometheus/prometheus/web/api/v1@2121b4628baa7d9d9406aa468712a6a332e77aff.

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/klauspost/compress/gzhttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"

	"github.com/thanos-io/thanos/pkg/extannotations"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type status string

const (
	StatusSuccess status = "success"
	StatusError   status = "error"
)

type ErrorType string

const (
	ErrorNone     ErrorType = ""
	ErrorTimeout  ErrorType = "timeout"
	ErrorCanceled ErrorType = "canceled"
	ErrorExec     ErrorType = "execution"
	ErrorBadData  ErrorType = "bad_data"
	ErrorInternal ErrorType = "internal"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Accept-Encoding, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

// ThanosVersion contains build information about Thanos.
type ThanosVersion struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

var BuildInfo = &ThanosVersion{
	Version:   version.Version,
	Revision:  version.Revision,
	Branch:    version.Branch,
	BuildUser: version.BuildUser,
	BuildDate: version.BuildDate,
	GoVersion: version.GoVersion,
}

type ApiError struct {
	Typ ErrorType
	Err error
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("%s: %s", e.Typ, e.Err)
}

// RuntimeInfo contains runtime information about Thanos.
type RuntimeInfo struct {
	StartTime      time.Time `json:"startTime"`
	CWD            string    `json:"CWD"`
	GoroutineCount int       `json:"goroutineCount"`
	GOMAXPROCS     int       `json:"GOMAXPROCS"`
	GOGC           string    `json:"GOGC"`
	GODEBUG        string    `json:"GODEBUG"`
}

// RuntimeInfoFn returns updated runtime information about Thanos.
type RuntimeInfoFn func() RuntimeInfo

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType ErrorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// SetCORS enables cross-site script calls.
func SetCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

type ApiFunc func(r *http.Request) (interface{}, []error, *ApiError, func())

type BaseAPI struct {
	logger      log.Logger
	flagsMap    map[string]string
	runtimeInfo RuntimeInfoFn
	buildInfo   *ThanosVersion
	Now         func() time.Time
	disableCORS bool
}

// NewBaseAPI returns a new initialized BaseAPI type.
func NewBaseAPI(logger log.Logger, disableCORS bool, flagsMap map[string]string) *BaseAPI {

	return &BaseAPI{
		logger:      logger,
		flagsMap:    flagsMap,
		runtimeInfo: GetRuntimeInfoFunc(logger),
		buildInfo:   BuildInfo,
		disableCORS: disableCORS,
		Now:         time.Now,
	}
}

// Register registers the common API endpoints.
func (api *BaseAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	instr := GetInstr(tracer, logger, ins, logMiddleware, api.disableCORS)

	r.Options("/*path", instr("options", api.options))

	r.Get("/status/flags", instr("status_flags", api.flags))
	r.Get("/status/runtimeinfo", instr("status_runtime", api.serveRuntimeInfo))
	r.Get("/status/buildinfo", instr("status_build", api.serveBuildInfo))
}

func (api *BaseAPI) options(r *http.Request) (interface{}, []error, *ApiError, func()) {
	return nil, nil, nil, func() {}
}

func (api *BaseAPI) flags(r *http.Request) (interface{}, []error, *ApiError, func()) {
	return api.flagsMap, nil, nil, func() {}
}

func (api *BaseAPI) serveRuntimeInfo(r *http.Request) (interface{}, []error, *ApiError, func()) {
	return api.runtimeInfo(), nil, nil, func() {}
}

func (api *BaseAPI) serveBuildInfo(r *http.Request) (interface{}, []error, *ApiError, func()) {
	return api.buildInfo, nil, nil, func() {}
}

func GetRuntimeInfoFunc(logger log.Logger) RuntimeInfoFn {
	CWD, err := os.Getwd()
	if err != nil {
		CWD = "<error retrieving current working directory>"
		level.Warn(logger).Log("msg", "failed to retrieve current working directory", "err", err)
	}

	birth := time.Now()

	return func() RuntimeInfo {
		return RuntimeInfo{
			StartTime:      birth,
			CWD:            CWD,
			GoroutineCount: runtime.NumGoroutine(),
			GOMAXPROCS:     runtime.GOMAXPROCS(0),
			GOGC:           os.Getenv("GOGC"),
			GODEBUG:        os.Getenv("GODEBUG"),
		}
	}
}

type InstrFunc func(name string, f ApiFunc) http.HandlerFunc

// GetInstr returns a http HandlerFunc with the instrumentation middleware.
func GetInstr(
	tracer opentracing.Tracer,
	logger log.Logger,
	ins extpromhttp.InstrumentationMiddleware,
	logMiddleware *logging.HTTPServerMiddleware,
	disableCORS bool,
) InstrFunc {
	instr := func(name string, f ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !disableCORS {
				SetCORS(w)
			}
			if data, warnings, err, releaseResources := f(r); err != nil {
				RespondError(w, err, data)
				releaseResources()
			} else if data != nil {
				Respond(w, data, warnings)
				releaseResources()
			} else {
				w.WriteHeader(http.StatusNoContent)
				releaseResources()
			}
		})

		return tracing.HTTPMiddleware(tracer, name, logger,
			ins.NewHandler(name,
				gzhttp.GzipHandler(
					middleware.RequestID(
						logMiddleware.HTTPMiddleware(name, hf),
					),
				),
			),
		)
	}
	return instr
}

func shouldNotCacheBecauseOfWarnings(warnings []error) bool {
	for _, w := range warnings {
		// PromQL warnings should not prevent caching
		if !extannotations.IsPromQLAnnotation(w.Error()) {
			return true
		}
	}
	return false
}

func Respond(w http.ResponseWriter, data interface{}, warnings []error) {
	w.Header().Set("Content-Type", "application/json")
	if shouldNotCacheBecauseOfWarnings(warnings) {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)

	resp := &response{
		Status:   StatusSuccess,
		Warnings: warningsToString(warnings),
		Data:     data,
	}
	for _, warn := range warnings {
		resp.Warnings = append(resp.Warnings, warn.Error())
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func RespondError(w http.ResponseWriter, apiErr *ApiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	var code int
	switch apiErr.Typ {
	case ErrorBadData:
		code = http.StatusBadRequest
	case ErrorExec:
		code = 422
	case ErrorCanceled, ErrorTimeout:
		code = http.StatusServiceUnavailable
	case ErrorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(&response{
		Status:    StatusError,
		ErrorType: apiErr.Typ,
		Error:     apiErr.Err.Error(),
		Data:      data,
	})
}

func warningsToString(errors []error) []string {
	warningStrings := make([]string, len(errors))

	for i, err := range errors {
		if err != nil {
			warningStrings[i] = err.Error()
		}
	}

	return warningStrings
}
