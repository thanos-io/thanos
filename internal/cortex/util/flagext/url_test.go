package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestURLValueYAML(t *testing.T) {
	// Test embedding of URLValue.
	{
		type TestStruct struct {
			URL URLValue `yaml:"url"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.URL.Set("http://google.com"))
		expected := []byte(`url: http://google.com
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test pointers of URLValue.
	{
		type TestStruct struct {
			URL *URLValue `yaml:"url"`
		}

		var testStruct TestStruct
		testStruct.URL = &URLValue{}
		require.NoError(t, testStruct.URL.Set("http://google.com"))
		expected := []byte(`url: http://google.com
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test no url set in URLValue.
	{
		type TestStruct struct {
			URL URLValue `yaml:"url"`
		}

		var testStruct TestStruct
		expected := []byte(`url: ""
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test passwords are masked.
	{
		type TestStruct struct {
			URL URLValue `yaml:"url"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.URL.Set("http://username:password@google.com"))
		expected := []byte(`url: http://username:%2A%2A%2A%2A%2A%2A%2A%2A@google.com
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}
