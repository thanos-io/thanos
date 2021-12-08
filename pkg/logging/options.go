// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"math/rand"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/ulid"
	"google.golang.org/grpc/codes"
)

// Decision defines rules for enabling start and end of logging.
type Decision int

const (
	// NoLogCall - Logging is disabled.
	NoLogCall Decision = iota
	// LogFinishCall - Only finish logs of request is enabled.
	LogFinishCall
	// LogStartAndFinishCall - Logging of start and end of request is enabled.
	LogStartAndFinishCall
)

var defaultOptions = &options{
	shouldLog:         DefaultDeciderMethod,
	codeFunc:          DefaultErrorToCode,
	levelFunc:         DefaultCodeToLevel,
	durationFieldFunc: DurationToTimeMillisFields,
	filterLog:         DefaultFilterLogging,
}

func evaluateOpt(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	optCopy.levelFunc = DefaultCodeToLevel
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

// WithDecider customizes the function for deciding if the HTTP Middlewares/Tripperwares should log.
func WithDecider(f Decider) Option {
	return func(o *options) {
		o.shouldLog = f
	}
}

// WithLevels customizes the function for mapping HTTP response codes and interceptor log level statements.
func WithLevels(f CodeToLevel) Option {
	return func(o *options) {
		o.levelFunc = f
	}
}

// WithFilter customizes the function for deciding which level of logging should be allowed.
// Follows go-kit Allow<level of log> convention.
func WithFilter(f FilterLogging) Option {
	return func(o *options) {
		o.filterLog = f
	}
}

// Interface for the additional methods.

// Types for the Options.
type Option func(*options)

// Fields represents logging fields. It has to have even number of elements (pairs).
type Fields []string

// ErrorToCode function determines the error code of the error
// for the http response.
type ErrorToCode func(err error) int

// DefaultErrorToCode returns an InternalServerError.
func DefaultErrorToCode(_ error) int {
	return 500
}

// Decider function defines rules for suppressing the logging.
type Decider func(methodName string, err error) Decision

// DefaultDeciderMethod is the default implementation of decider to see if you should log the call
// by default this is set to LogStartAndFinishCall.
func DefaultDeciderMethod(_ string, _ error) Decision {
	return LogStartAndFinishCall
}

// CodeToLevel function defines the mapping between HTTP Response codes to log levels for server side.
type CodeToLevel func(logger log.Logger, code int) log.Logger

// DurationToFields function defines how to produce duration fields for logging.
type DurationToFields func(duration time.Duration) Fields

// FilterLogging makes sure only the logs with level=lvl gets logged, or filtered.
type FilterLogging func(logger log.Logger) log.Logger

// DefaultFilterLogging allows logs from all levels to be logged in output.
func DefaultFilterLogging(logger log.Logger) log.Logger {
	return level.NewFilter(logger, level.AllowAll())
}

type options struct {
	levelFunc         CodeToLevel
	shouldLog         Decider
	codeFunc          ErrorToCode
	durationFieldFunc DurationToFields
	filterLog         FilterLogging
}

// DefaultCodeToLevel is the helper mapper that maps HTTP Response codes to log levels.
func DefaultCodeToLevel(logger log.Logger, code int) log.Logger {
	if code >= 200 && code < 500 {
		return level.Debug(logger)
	}
	return level.Error(logger)
}

// DefaultCodeToLevelGRPC is the helper mapper that maps gRPC Response codes to log levels.
func DefaultCodeToLevelGRPC(c codes.Code) grpc_logging.Level {
	switch c {
	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return grpc_logging.ERROR
	default:
		return grpc_logging.DEBUG
	}
}

// DurationToTimeMillisFields converts the duration to milliseconds and uses the key `http.time_ms`.
func DurationToTimeMillisFields(duration time.Duration) Fields {
	return Fields{"http.time_ms", fmt.Sprintf("%v", durationToMilliseconds(duration))}
}

func durationToMilliseconds(duration time.Duration) float32 {
	return float32(duration.Nanoseconds()/1000) / 1000
}

// LogDecision defines mapping of flag options to the logging decision.
var LogDecision = map[string]Decision{
	"NoLogCall":             NoLogCall,
	"LogFinishCall":         LogFinishCall,
	"LogStartAndFinishCall": LogStartAndFinishCall,
}

// MapAllowedLevels allows to map a given level to a list of allowed level.
// Convention taken from go-kit/level v0.10.0 https://godoc.org/github.com/go-kit/log/level#AllowAll.
var MapAllowedLevels = map[string][]string{
	"DEBUG": {"INFO", "DEBUG", "WARN", "ERROR"},
	"ERROR": {"ERROR"},
	"INFO":  {"INFO", "WARN", "ERROR"},
	"WARN":  {"WARN", "ERROR"},
}

// TODO: @yashrsharma44 - To be deprecated in the next release.
func ParseHTTPOptions(flagDecision string, reqLogConfig *extflag.PathOrContent) ([]Option, error) {
	// Default Option: No Logging.
	logOpts := []Option{WithDecider(func(_ string, _ error) Decision {
		return NoLogCall
	})}

	// If flag is incorrectly parsed.
	configYAML, err := reqLogConfig.Content()
	if err != nil {
		return logOpts, fmt.Errorf("getting request logging config failed. %v", err)
	}

	// Check if the user enables request logging through flags and YAML.
	if len(configYAML) != 0 && flagDecision != "" {
		return logOpts, fmt.Errorf("both log.request.decision and request.logging have been enabled, please use only one of the flags")
	}
	// If old flag is enabled.
	if len(flagDecision) > 0 {
		logOpts := []Option{WithDecider(func(_ string, _ error) Decision {
			return LogDecision[flagDecision]
		})}
		return logOpts, nil
	}
	return NewHTTPOption(configYAML)
}

// TODO: @yashrsharma44 - To be deprecated in the next release.
func ParsegRPCOptions(flagDecision string, reqLogConfig *extflag.PathOrContent) ([]tags.Option, []grpc_logging.Option, error) {
	// Default Option: No Logging.
	logOpts := []grpc_logging.Option{grpc_logging.WithDecider(func(_ string, _ error) grpc_logging.Decision {
		return grpc_logging.NoLogCall
	})}

	configYAML, err := reqLogConfig.Content()
	if err != nil {
		return []tags.Option{}, logOpts, fmt.Errorf("getting request logging config failed. %v", err)
	}

	// Check if the user enables request logging through flags and YAML.
	if len(configYAML) != 0 && flagDecision != "" {
		return []tags.Option{}, logOpts, fmt.Errorf("both log.request.decision and request.logging-config have been enabled, please use only one of the flags")
	}

	// If the old flag is empty, use the new YAML config.
	if flagDecision == "" {
		tagOpts, logOpts, err := NewGRPCOption(configYAML)
		if err != nil {
			return []tags.Option{}, logOpts, err
		}
		return tagOpts, logOpts, nil
	}

	tagOpts := []tags.Option{
		tags.WithFieldExtractor(func(_ string, req interface{}) map[string]string {
			tagMap := tags.TagBasedRequestFieldExtractor("request-id")("", req)
			// If a request-id exists for a given request.
			if tagMap != nil {
				if _, ok := tagMap["request-id"]; ok {
					return tagMap
				}
			}
			entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
			reqID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
			tagMap = make(map[string]string)
			tagMap["request-id"] = reqID.String()
			return tagMap
		}),
	}
	logOpts = []grpc_logging.Option{grpc_logging.WithDecider(func(_ string, _ error) grpc_logging.Decision {
		switch flagDecision {
		case "NoLogCall":
			return grpc_logging.NoLogCall
		case "LogFinishCall":
			return grpc_logging.LogFinishCall
		case "LogStartAndFinishCall":
			return grpc_logging.LogStartAndFinishCall
		default:
			return grpc_logging.NoLogCall
		}
	})}
	return tagOpts, logOpts, nil
}
