package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDayValueYAML(t *testing.T) {
	t.Run("embedding DayValue", func(t *testing.T) {
		type TestStruct struct {
			Day DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.Day.Set("1985-06-02"))
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	})

	t.Run("pointer of DayValue", func(t *testing.T) {
		type TestStruct struct {
			Day *DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		testStruct.Day = &DayValue{}
		require.NoError(t, testStruct.Day.Set("1985-06-02"))
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	})
}
