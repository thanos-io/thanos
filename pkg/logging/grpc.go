package logging

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/ulid"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

// NewReqLogConfig parses the string into a req logging config structure.
// Raise an error if unmarshalling is not possible, or values are not valid.
func NewReqLogConfig(configYAML []byte) (*ReqLogConfig, error) {

	reqLogConfig := &ReqLogConfig{}

	if err := yaml.UnmarshalStrict(configYAML, reqLogConfig); err != nil {
		return nil, err
	}
	// Lets assume all values are correct. Some datatypes are enforced by ummarshal,
	// while methods/services/path are assumed to be correct.

	// TODO: @yashrsharma44 - discuss if more checks are required
	return reqLogConfig, nil
}

// Checks if the OptionsConfig struct is empty and valid.
// If invalid combination is present, return an error.
func checkOptionsConfigEmpty(optcfg OptionsConfig) (bool, error) {
	if len(optcfg.Level) == 0 && !optcfg.Decision.LogEnd && !optcfg.Decision.LogStart {
		return true, nil
	}

	if len(optcfg.Level) == 0 && (optcfg.Decision.LogStart || optcfg.Decision.LogEnd) {
		return false, fmt.Errorf("level field is empty.")
	}

	return false, nil
}

// This function configures all the method to have global config for logging.
func fillGlobalOptionConfig(reqLogConfig *ReqLogConfig, isgRPC bool) (string, bool, bool, error) {

	var globalLevel string
	globalStart, globalEnd := false, false

	globalOptionConfig := reqLogConfig.Options
	isEmpty, err := checkOptionsConfigEmpty(globalOptionConfig)

	// If the decision for logging is enabled with empty level field.
	if err != nil {
		return "", false, false, err
	}

	if !isEmpty {
		globalLevel = globalOptionConfig.Level
		globalStart = globalOptionConfig.Decision.LogStart
		globalEnd = globalOptionConfig.Decision.LogEnd
	}
	var protocolOptionConfig OptionsConfig
	if isgRPC {
		// gRPC config overrides the global config.
		protocolOptionConfig = reqLogConfig.GRPC.Options
	} else {
		// gRPC config overrides the global config.
		protocolOptionConfig = reqLogConfig.HTTP.Options
	}

	isEmpty, err = checkOptionsConfigEmpty(protocolOptionConfig)

	// If the decision for logging is enabled with empty level field.
	if err != nil {
		return "", false, false, err
	}

	if !isEmpty {
		globalLevel = protocolOptionConfig.Level
		globalStart = protocolOptionConfig.Decision.LogStart
		globalEnd = protocolOptionConfig.Decision.LogEnd
	}

	return globalLevel, globalStart, globalEnd, nil

}

// Returns the logging ENUM based on logStart and logEnd values.
func getGRPCLoggingOption(logStart bool, logEnd bool) (grpc_logging.Decision, error) {
	if !logStart && !logEnd {
		return grpc_logging.NoLogCall, nil
	}

	if !logStart && logEnd {
		return grpc_logging.LogFinishCall, nil
	}

	if logStart && logEnd {
		return grpc_logging.LogFinishCall, nil
	}

	return -1, fmt.Errorf("log start call is not supported.")
}

// Validate the list of level entries.
// Raise an error if empty or log level not in uppercase.
func validateLevel(level string) error {
	if len(level) == 0 {
		return fmt.Errorf("level field in YAML file is empty.")
	}

	if level == "INFO" || level == "DEBUG" || level == "ERROR" || level == "WARNING" {
		return nil
	}

	return fmt.Errorf("The format of level is invalid. Expected INFO/DEBUG/ERROR/WARNING, got this %v", level)

}

// NewGRPCLoggingOption adds in the config options and returns tags for logging middleware.
func NewGRPCLoggingOption(configYAML []byte) ([]tags.Option, []grpc_logging.Option, error) {

	// Configure tagOpts and logOpts
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

	logOpts := []grpc_logging.Option{}

	// Unmarshal YAML
	// if req logging is disabled.
	if len(configYAML) == 0 {
		return []tags.Option{}, []grpc_logging.Option{}, nil
	}

	reqLogConfig, err := NewReqLogConfig(configYAML)

	// If unmarshalling is an issue.
	if err != nil {
		return []tags.Option{}, []grpc_logging.Option{}, err
	}

	globalLevel, globalStart, globalEnd, err := fillGlobalOptionConfig(reqLogConfig, true)

	// If global options have invalid entries.
	if err != nil {
		return tagOpts, logOpts, err
	}

	// If the level entry does not matches our entries.
	if err := validateLevel(globalLevel); err != nil {
		return tagOpts, logOpts, err
	}

	// If the combination is valid, use them, otherwise return error.
	reqLogDecision, err := getGRPCLoggingOption(globalStart, globalEnd)
	if err != nil {
		return tagOpts, logOpts, err
	}

	if len(reqLogConfig.GRPC.Config) == 0 {
		logOpts = []grpc_logging.Option{
			grpc_logging.WithDecider(func(_ string, err error) grpc_logging.Decision {

				runtimeLevel := grpc_logging.DefaultServerCodeToLevel(status.Code(err))
				if string(runtimeLevel) == strings.ToLower(globalLevel) {
					return reqLogDecision
				}

				return grpc_logging.NoLogCall
			}),
			grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
		}

		return tagOpts, logOpts, nil
	}

	logOpts = []grpc_logging.Option{
		grpc_logging.WithLevels(DefaultCodeToLevelGRPC),
	}

	for _, eachConfig := range reqLogConfig.GRPC.Config {
		eachConfigMethodName := interceptors.FullMethod(eachConfig.Service, eachConfig.Method)

		logOpts = append(logOpts, []grpc_logging.Option{
			grpc_logging.WithDecider(func(runtimeMethodName string, err error) grpc_logging.Decision {

				if runtimeMethodName == eachConfigMethodName {
					runtimeLevel := grpc_logging.DefaultServerCodeToLevel(status.Code(err))

					if string(runtimeLevel) == strings.ToLower(globalLevel) {
						return reqLogDecision
					}

				}
				return grpc_logging.NoLogCall
			}),
		}...)
	}
	return tagOpts, logOpts, nil
}
