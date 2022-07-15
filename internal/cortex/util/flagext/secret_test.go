package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSecretdYAML(t *testing.T) {
	// Test embedding of Secret.
	{
		type TestStruct struct {
			Secret Secret `yaml:"secret"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.Secret.Set("pa55w0rd"))
		expected := []byte(`secret: '********'
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		yamlSecret := []byte(`secret: pa55w0rd
`)
		err = yaml.Unmarshal(yamlSecret, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test pointers of Secret.
	{
		type TestStruct struct {
			Secret *Secret `yaml:"secret"`
		}

		var testStruct TestStruct
		testStruct.Secret = &Secret{}
		require.NoError(t, testStruct.Secret.Set("pa55w0rd"))
		expected := []byte(`secret: '********'
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		yamlSecret := []byte(`secret: pa55w0rd
`)
		err = yaml.Unmarshal(yamlSecret, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test no value set in Secret.
	{
		type TestStruct struct {
			Secret Secret `yaml:"secret"`
		}
		var testStruct TestStruct
		expected := []byte(`secret: ""
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
}
