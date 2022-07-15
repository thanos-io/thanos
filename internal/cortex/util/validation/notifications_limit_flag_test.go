// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package validation

import (
	"bytes"
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestNotificationLimitsMap(t *testing.T) {
	for name, tc := range map[string]struct {
		args     []string
		expected NotificationRateLimitMap
		error    string
	}{
		"basic test": {
			args: []string{"-map-flag", "{\"email\": 100 }"},
			expected: NotificationRateLimitMap{
				"email": 100,
			},
		},

		"unknown integration": {
			args:  []string{"-map-flag", "{\"unknown\": 200 }"},
			error: "invalid value \"{\\\"unknown\\\": 200 }\" for flag -map-flag: unknown integration name: unknown",
		},

		"parsing error": {
			args:  []string{"-map-flag", "{\"hello\": ..."},
			error: "invalid value \"{\\\"hello\\\": ...\" for flag -map-flag: invalid character '.' looking for beginning of value",
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := NotificationRateLimitMap{}

			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(&bytes.Buffer{}) // otherwise errors would go to stderr.
			fs.Var(v, "map-flag", "Map flag, you can pass JSON into this")
			err := fs.Parse(tc.args)

			if tc.error != "" {
				require.NotNil(t, err)
				assert.Equal(t, tc.error, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, v)
			}
		})
	}
}

type TestStruct struct {
	Flag NotificationRateLimitMap `yaml:"flag"`
}

func TestNotificationsLimitMapYaml(t *testing.T) {

	var testStruct TestStruct
	testStruct.Flag = map[string]float64{}

	require.NoError(t, testStruct.Flag.Set("{\"email\": 500 }"))
	expected := []byte(`flag:
  email: 500
`)

	actual, err := yaml.Marshal(testStruct)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	var actualStruct TestStruct
	actualStruct.Flag = NotificationRateLimitMap{} // must be set, otherwise unmarshalling panics.

	err = yaml.Unmarshal(expected, &actualStruct)
	require.NoError(t, err)
	assert.Equal(t, testStruct, actualStruct)
}

func TestUnknownIntegrationWhenLoadingYaml(t *testing.T) {
	var s TestStruct
	s.Flag = NotificationRateLimitMap{} // must be set, otherwise unmarshalling panics.

	yamlInput := `flag:
  unknown_integration: 500
`

	err := yaml.Unmarshal([]byte(yamlInput), &s)
	require.NotNil(t, err)
	require.Equal(t, "unknown integration name: unknown_integration", err.Error())
}

func TestWrongYamlStructureWhenLoadingYaml(t *testing.T) {
	var s TestStruct
	s.Flag = NotificationRateLimitMap{} // must be set, otherwise unmarshalling panics.

	yamlInput := `flag:
  email:
    rate_limit: 7777
    burst_size: 7777
`

	err := yaml.Unmarshal([]byte(yamlInput), &s)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "cannot unmarshal !!map into float64")
}
