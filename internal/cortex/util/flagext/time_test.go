package flagext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestTimeYAML(t *testing.T) {
	{
		type TestStruct struct {
			T Time `yaml:"time"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.T.Set("2020-10-20"))

		marshaled, err := yaml.Marshal(testStruct)
		require.NoError(t, err)

		expected := `time: "2020-10-20T00:00:00Z"` + "\n"
		assert.Equal(t, expected, string(marshaled))

		var actualStruct TestStruct
		err = yaml.Unmarshal([]byte(expected), &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	{
		type TestStruct struct {
			T *Time `yaml:"time"`
		}

		var testStruct TestStruct
		testStruct.T = &Time{}
		require.NoError(t, testStruct.T.Set("2020-10-20"))

		marshaled, err := yaml.Marshal(testStruct)
		require.NoError(t, err)

		expected := `time: "2020-10-20T00:00:00Z"` + "\n"
		assert.Equal(t, expected, string(marshaled))

		var actualStruct TestStruct
		err = yaml.Unmarshal([]byte(expected), &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
}

func TestTimeFormats(t *testing.T) {
	ts := &Time{}
	require.NoError(t, ts.Set("0"))
	require.True(t, time.Time(*ts).IsZero())
	require.Equal(t, "0", ts.String())

	require.NoError(t, ts.Set("2020-10-05"))
	require.Equal(t, mustParseTime(t, "2006-01-02", "2020-10-05").Format(time.RFC3339), ts.String())

	require.NoError(t, ts.Set("2020-10-05T13:27"))
	require.Equal(t, mustParseTime(t, "2006-01-02T15:04", "2020-10-05T13:27").Format(time.RFC3339), ts.String())

	require.NoError(t, ts.Set("2020-10-05T13:27:00Z"))
	require.Equal(t, mustParseTime(t, "2006-01-02T15:04:05Z", "2020-10-05T13:27:00Z").Format(time.RFC3339), ts.String())

	require.NoError(t, ts.Set("2020-10-05T13:27:00+02:00"))
	require.Equal(t, mustParseTime(t, "2006-01-02T15:04:05Z07:00", "2020-10-05T13:27:00+02:00").Format(time.RFC3339), ts.String())
}

func mustParseTime(t *testing.T, f, s string) time.Time {
	ts, err := time.Parse(f, s)
	if err != nil {
		t.Fatalf("failed to parse %q: %v", s, err)
	}
	return ts
}
