// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"

	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	httputil "github.com/thanos-io/thanos/pkg/server/http"
)

type HTTPServerMiddleware struct {
	opts   *options
	logger log.Logger
}

func (m *HTTPServerMiddleware) preCall(start time.Time) {
	logger := m.opts.filterLog(m.logger)
	level.Debug(logger).Log("http.start_time", start.String(), "msg", "started call")
}

func (m *HTTPServerMiddleware) postCall(name string, start time.Time, wrapped *httputil.ResponseWriterWithStatus, r *http.Request) {
	status := wrapped.Status()
	logger := log.With(m.logger, "http.method", name, "http.request.id", r.Header.Get("X-Request-ID"), "http.code", fmt.Sprintf("%d", status),
		"http.time_ms", fmt.Sprintf("%v", durationToMilliseconds(time.Since(start))))

	if status >= 500 && status < 600 {
		logger = log.With(logger, "http.error", fmt.Sprintf("%v", status))
	}
	logger = m.opts.filterLog(logger)
	m.opts.levelFunc(logger, status).Log("msg", "finished call")
}

func (m *HTTPServerMiddleware) HTTPMiddleware(name string, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped := httputil.WrapResponseWriterWithStatus(w)
		start := time.Now()
		decision := m.opts.shouldLog(name, nil)

		switch decision {
		case NoLogCall:
			next.ServeHTTP(w, r)

		case LogStartAndFinishCall:
			m.preCall(start)
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)

		case LogFinishCall:
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)
		}
	}
}

func NewHTTPServerMiddleware(logger log.Logger, opts ...Option) *HTTPServerMiddleware {
	o := evaluateOpt(opts)
	return &HTTPServerMiddleware{
		logger: log.With(logger, "protocol", "http", "http.component", "server"),
		opts:   o,
	}
}

// Returns the logging ENUM based on logStart and logEnd values.
func getHTTPLoggingOption(logStart bool, logEnd bool) (Decision, error) {
	if !logStart && !logEnd {
		return NoLogCall, nil
	}

	if !logStart && logEnd {
		return LogFinishCall, nil
	}

	if logStart && logEnd {
		return LogFinishCall, nil
	}

	return -1, fmt.Errorf("log start call is not supported.")
}

func getLevel(lvl string) level.Option {
	switch lvl {
	case "INFO":
		return level.AllowInfo()
	case "DEBUG":
		return level.AllowDebug()
	case "WARN":
		return level.AllowWarn()
	case "ERROR":
		return level.AllowError()
	default:
		return level.AllowAll()
	}
}

func NewHTTPLoggingOption(configYAML []byte) ([]Option, error) {

	// Define a black config option.
	var logOpts []Option

	// If req logging is disabled.
	if len(configYAML) == 0 {
		return []Option{}, nil
	}

	reqLogConfig, err := NewReqLogConfig(configYAML)

	// If unmarshalling is an issue.
	if err != nil {
		return []Option{}, err
	}

	globalLevel, globalStart, globalEnd, err := fillGlobalOptionConfig(reqLogConfig, false)

	// If global options have invalid entries.
	if err != nil {
		return []Option{}, err
	}

	// If the level entry does not matches our entries.
	if err := validateLevel(globalLevel); err != nil {
		return []Option{}, err
	}

	// If the combination is valid, use them, otherwise return error.
	reqLogDecision, err := getHTTPLoggingOption(globalStart, globalEnd)
	if err != nil {
		return []Option{}, err
	}

	logOpts = []Option{
		WithFilter(func(logger log.Logger) log.Logger {
			return level.NewFilter(logger, getLevel(globalLevel))
		}),
		WithLevels(DefaultCodeToLevel),
	}

	if len(reqLogConfig.HTTP.Config) == 0 {
		logOpts = append(logOpts, []Option{WithDecider(func(_ string, err error) Decision {
			return reqLogDecision
		}),
		}...)
		return logOpts, nil
	}

	for _, eachConfig := range reqLogConfig.HTTP.Config {
		eachConfigName := fmt.Sprintf("%v:%v", eachConfig.Method, eachConfig.Port)

		logOpts = append(logOpts, []Option{
			WithDecider(func(runtimeMethodName string, err error) Decision {
				if eachConfigName == runtimeMethodName {
					return reqLogDecision
				}
				return NoLogCall
			}),
		}...)

	}

	return logOpts, nil

}
