package store

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"gopkg.in/alecthomas/kingpin.v2"
)

// TimeOrDurationValue is a custom kingping parser for time in RFC3339
// or duration in Go's duration format, such as "300ms", "-1.5h" or "2h45m".
// Only one will be set.
type TimeOrDurationValue struct {
	t   *time.Time
	dur *model.Duration
}

// Set converts string to TimeOrDurationValue
func (tdv *TimeOrDurationValue) Set(s string) error {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		dur, err := model.ParseDuration(s)
		if err != nil {
			return err
		}

		tdv.dur = &dur
		return nil
	}

	tdv.t = &t
	return nil
}

// String returns either tume or duration
func (tdv *TimeOrDurationValue) String() string {
	switch {
	case tdv.t != nil:
		return tdv.t.String()
	case tdv.dur != nil:
		return tdv.dur.String()
	}

	return "nil"
}

// PrometheusTimestamp returns TimeOrDurationValue converted to PrometheusTimestamp
// if duration is set now+duration is converted to Timestamp.
func (tdv *TimeOrDurationValue) PrometheusTimestamp() int64 {
	switch {
	case tdv.t != nil:
		return timestamp.FromTime(*tdv.t)
	case tdv.dur != nil:
		return timestamp.FromTime(time.Now().Add(time.Duration(*tdv.dur)))
	}

	return 0
}

// TimeOrDuration helper for parsing TimeOrDuration with kingpin
func TimeOrDuration(flags *kingpin.FlagClause) *TimeOrDurationValue {
	value := new(TimeOrDurationValue)
	flags.SetValue(value)
	return value
}
