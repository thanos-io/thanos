// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestValidateLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		level   string
		wantErr bool
	}{
		{name: "INFO is valid", level: "INFO", wantErr: false},
		{name: "DEBUG is valid", level: "DEBUG", wantErr: false},
		{name: "ERROR is valid", level: "ERROR", wantErr: false},
		{name: "WARNING is valid", level: "WARNING", wantErr: false},
		{name: "WARN is invalid", level: "WARN", wantErr: true},
		{name: "empty is invalid", level: "", wantErr: true},
		{name: "lowercase info is invalid", level: "info", wantErr: true},
		{name: "unknown level is invalid", level: "TRACE", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateLevel(tc.level)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}

func TestGetGRPCLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		logStart bool
		logEnd   bool
		wantErr  bool
	}{
		{name: "both false returns empty events", logStart: false, logEnd: false, wantErr: false},
		{name: "only logEnd returns FinishCall", logStart: false, logEnd: true, wantErr: false},
		{name: "both true returns StartCall and FinishCall", logStart: true, logEnd: true, wantErr: false},
		{name: "only logStart returns error", logStart: true, logEnd: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := getGRPCLoggingOption(tc.logStart, tc.logEnd)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}

func TestGetHTTPLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		logStart bool
		logEnd   bool
		wantErr  bool
		wantDec  Decision
	}{
		{name: "both false returns NoLogCall", logStart: false, logEnd: false, wantErr: false, wantDec: NoLogCall},
		{name: "only logEnd returns LogFinishCall", logStart: false, logEnd: true, wantErr: false, wantDec: LogFinishCall},
		{name: "both true returns LogStartAndFinishCall", logStart: true, logEnd: true, wantErr: false, wantDec: LogStartAndFinishCall},
		{name: "only logStart returns error", logStart: true, logEnd: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dec, err := getHTTPLoggingOption(tc.logStart, tc.logEnd)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.wantDec, dec)
			}
		})
	}
}

func TestCheckOptionsConfigEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       OptionsConfig
		wantEmpty bool
		wantErr   bool
	}{
		{
			name:      "all zero values is empty",
			cfg:       OptionsConfig{},
			wantEmpty: true,
			wantErr:   false,
		},
		{
			name: "level set with decisions is not empty",
			cfg: OptionsConfig{
				Level:    "INFO",
				Decision: DecisionConfig{LogStart: true, LogEnd: true},
			},
			wantEmpty: false,
			wantErr:   false,
		},
		{
			name: "logStart true but no level returns error",
			cfg: OptionsConfig{
				Decision: DecisionConfig{LogStart: true},
			},
			wantEmpty: false,
			wantErr:   true,
		},
		{
			name: "logEnd true but no level returns error",
			cfg: OptionsConfig{
				Decision: DecisionConfig{LogEnd: true},
			},
			wantEmpty: false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			isEmpty, err := checkOptionsConfigEmpty(tc.cfg)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.wantEmpty, isEmpty)
			}
		})
	}
}

func TestFillGlobalOptionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		config     *RequestConfig
		isGRPC     bool
		wantLevel  string
		wantStart  bool
		wantEnd    bool
		wantErr    bool
	}{
		{
			name:      "empty config returns defaults",
			config:    &RequestConfig{},
			isGRPC:    true,
			wantLevel: "ERROR",
			wantStart: false,
			wantEnd:   false,
			wantErr:   false,
		},
		{
			name: "global options are used",
			config: &RequestConfig{
				Options: OptionsConfig{
					Level:    "INFO",
					Decision: DecisionConfig{LogStart: true, LogEnd: true},
				},
			},
			isGRPC:    true,
			wantLevel: "INFO",
			wantStart: true,
			wantEnd:   true,
			wantErr:   false,
		},
		{
			name: "grpc options override global",
			config: &RequestConfig{
				Options: OptionsConfig{
					Level:    "INFO",
					Decision: DecisionConfig{LogStart: true, LogEnd: true},
				},
				GRPC: GRPCProtocolConfigs{
					Options: OptionsConfig{
						Level:    "DEBUG",
						Decision: DecisionConfig{LogStart: false, LogEnd: true},
					},
				},
			},
			isGRPC:    true,
			wantLevel: "DEBUG",
			wantStart: false,
			wantEnd:   true,
			wantErr:   false,
		},
		{
			name: "http options override global when not grpc",
			config: &RequestConfig{
				Options: OptionsConfig{
					Level:    "INFO",
					Decision: DecisionConfig{LogStart: true, LogEnd: true},
				},
				HTTP: HTTPProtocolConfigs{
					Options: OptionsConfig{
						Level:    "WARNING",
						Decision: DecisionConfig{LogStart: false, LogEnd: true},
					},
				},
			},
			isGRPC:    false,
			wantLevel: "WARNING",
			wantStart: false,
			wantEnd:   true,
			wantErr:   false,
		},
		{
			name: "global decision without level returns error",
			config: &RequestConfig{
				Options: OptionsConfig{
					Decision: DecisionConfig{LogStart: true},
				},
			},
			isGRPC:  true,
			wantErr: true,
		},
		{
			name: "protocol decision without level returns error",
			config: &RequestConfig{
				GRPC: GRPCProtocolConfigs{
					Options: OptionsConfig{
						Decision: DecisionConfig{LogEnd: true},
					},
				},
			},
			isGRPC:  true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lvl, start, end, err := fillGlobalOptionConfig(tc.config, tc.isGRPC)
			if tc.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.wantLevel, lvl)
				testutil.Equals(t, tc.wantStart, start)
				testutil.Equals(t, tc.wantEnd, end)
			}
		})
	}
}
