package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func Test_CIDRSliceCSV_YamlMarshalling(t *testing.T) {
	type TestStruct struct {
		CIDRs CIDRSliceCSV `yaml:"cidrs"`
	}

	tests := map[string]struct {
		input    string
		expected []string
	}{
		"should marshal empty config": {
			input:    "cidrs: \"\"\n",
			expected: nil,
		},
		"should marshal single value": {
			input:    "cidrs: 127.0.0.1/32\n",
			expected: []string{"127.0.0.1/32"},
		},
		"should marshal multiple comma-separated values": {
			input:    "cidrs: 127.0.0.1/32,10.0.10.0/28,fdf8:f53b:82e4::/100,192.168.0.0/20\n",
			expected: []string{"127.0.0.1/32", "10.0.10.0/28", "fdf8:f53b:82e4::/100", "192.168.0.0/20"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Unmarshal.
			actual := TestStruct{}
			err := yaml.Unmarshal([]byte(tc.input), &actual)
			assert.NoError(t, err)

			assert.Len(t, actual.CIDRs, len(tc.expected))
			for idx, cidr := range actual.CIDRs {
				assert.Equal(t, tc.expected[idx], cidr.String())
			}

			// Marshal.
			out, err := yaml.Marshal(actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.input, string(out))
		})
	}
}
