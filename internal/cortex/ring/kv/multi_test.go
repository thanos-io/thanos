package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func boolPtr(b bool) *bool {
	return &b
}

func TestMultiRuntimeConfigWithVariousEnabledValues(t *testing.T) {
	testcases := map[string]struct {
		yaml     string
		expected *bool
	}{
		"nil":   {"primary: test", nil},
		"true":  {"primary: test\nmirror_enabled: true", boolPtr(true)},
		"false": {"mirror_enabled: false", boolPtr(false)},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			c := MultiRuntimeConfig{}
			err := yaml.Unmarshal([]byte(tc.yaml), &c)
			assert.NoError(t, err, tc.yaml)
			assert.Equal(t, tc.expected, c.Mirroring, tc.yaml)
		})
	}
}
