// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"net"
	"sort"
	"strings"

	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	httputil "github.com/thanos-io/thanos/pkg/server/http"
)

type HTTPServerMiddleware struct {
	opts   *options
	logger log.Logger
}

func (m *HTTPServerMiddleware) preCall(name string, start time.Time, r *http.Request) {
	logger := m.opts.filterLog(m.logger)
	level.Debug(logger).Log("http.start_time", start.String(), "http.method", fmt.Sprintf("%s %s", r.Method, r.URL), "http.request_id", r.Header.Get("X-Request-ID"), "thanos.method_name", name, "msg", "started call")
}

func (m *HTTPServerMiddleware) postCall(name string, start time.Time, wrapped *httputil.ResponseWriterWithStatus, r *http.Request) {
	status := wrapped.Status()
	logger := log.With(m.logger, "http.method", fmt.Sprintf("%s %s", r.Method, r.URL), "http.request_id", r.Header.Get("X-Request-ID"), "http.status_code", fmt.Sprintf("%d", status),
		"http.time_ms", fmt.Sprintf("%v", durationToMilliseconds(time.Since(start))), "http.remote_addr", r.RemoteAddr, "thanos.method_name", name)

	logger = m.opts.filterLog(logger)
	m.opts.levelFunc(logger, status).Log("msg", "finished call")
}

func (m *HTTPServerMiddleware) HTTPMiddleware(name string, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped := httputil.WrapResponseWriterWithStatus(w)
		start := time.Now()
		hostPort := r.Host
		if hostPort == "" {
			hostPort = r.URL.Host
		}

		var port string
		var err error
		// Try to extract port if there is ':' as part of 'hostPort'.
		if strings.Contains(hostPort, ":") {
			_, port, err = net.SplitHostPort(hostPort)
			if err != nil {
				level.Error(m.logger).Log("msg", "failed to parse host port for http log decision", "err", err)
				next.ServeHTTP(w, r)
				return
			}
		}

		deciderURL := r.URL.String()
		if len(port) > 0 {
			deciderURL = net.JoinHostPort(deciderURL, port)
		}
		decision := m.opts.shouldLog(deciderURL, nil)

		switch decision {
		case NoLogCall:
			next.ServeHTTP(w, r)

		case LogStartAndFinishCall:
			m.preCall(name, start, r)
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)

		case LogFinishCall:
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)
		}
	}
}

// NewHTTPServerMiddleware returns an http middleware.
func NewHTTPServerMiddleware(logger log.Logger, opts ...Option) *HTTPServerMiddleware {
	o := evaluateOpt(opts)
	return &HTTPServerMiddleware{
		logger: log.With(logger, "protocol", "http", "http.component", "server"),
		opts:   o,
	}
}

// getHTTPLoggingOption returns the logging ENUM based on logStart and logEnd values.
func getHTTPLoggingOption(logStart, logEnd bool) (Decision, error) {
	if !logStart && !logEnd {
		return NoLogCall, nil
	}
	if !logStart && logEnd {
		return LogFinishCall, nil
	}
	if logStart && logEnd {
		return LogStartAndFinishCall, nil
	}
	return -1, fmt.Errorf("log start call is not supported")
}

// getLevel returns the level based logger.
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

// NewHTTPOption returns a http config option.
func NewHTTPOption(configYAML []byte) ([]Option, error) {
	// Define a black config option.
	logOpts := []Option{
		WithDecider(func(_ string, err error) Decision {
			return NoLogCall
		}),
	}

	// If req logging is disabled.
	if len(configYAML) == 0 {
		return logOpts, nil
	}

	reqLogConfig, err := NewRequestConfig(configYAML)
	// If unmarshalling is an issue.
	if err != nil {
		return logOpts, err
	}

	globalLevel, globalStart, globalEnd, err := fillGlobalOptionConfig(reqLogConfig, false)

	// If global options have invalid entries.
	if err != nil {
		return logOpts, err
	}
	// If the level entry does not matches our entries.
	if err := validateLevel(globalLevel); err != nil {
		// fmt.Printf("HTTP")
		return logOpts, err
	}

	// If the combination is valid, use them, otherwise return error.
	reqLogDecision, err := getHTTPLoggingOption(globalStart, globalEnd)
	if err != nil {
		return logOpts, err
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

	methodNameSlice := []string{}

	for _, eachConfig := range reqLogConfig.HTTP.Config {
		eachConfigName := fmt.Sprintf("%v:%v", eachConfig.Path, eachConfig.Port)
		methodNameSlice = append(methodNameSlice, eachConfigName)
	}

	sort.Strings(methodNameSlice)

	logOpts = append(logOpts, []Option{
		WithDecider(func(runtimeMethodName string, err error) Decision {
			idx := sort.SearchStrings(methodNameSlice, runtimeMethodName)
			if idx < len(methodNameSlice) && methodNameSlice[idx] == runtimeMethodName {
				return reqLogDecision
			}
			return NoLogCall
		}),
	}...)
	return logOpts, nil

}
