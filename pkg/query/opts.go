package query

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const (
	dedupParam               = "dedup"
	partialResponseParam     = "partial_response"
	maxSourceResolutionParam = "max_source_resolution"
	// TODO(bwplotka): Add new partial response strategy and resolution parsing to QueryAPI
)

func EncodePartialResponseStrategy(p storepb.PartialResponseStrategy, values url.Values) error {
	var partialResponseValue string
	switch p {
	case storepb.PartialResponseStrategy_WARN:
		partialResponseValue = strconv.FormatBool(true)
	case storepb.PartialResponseStrategy_ABORT:
		partialResponseValue = strconv.FormatBool(false)
	default:
		return errors.Errorf("unknown partial response strategy %v", p)
	}

	// TODO(bwplotka): Apply change from bool to strategy in Query API as well.
	values.Add(partialResponseParam, partialResponseValue)
	return nil
}

func NewPartialResponseStrategyFromForm(values url.Values, defEnable bool) (storepb.PartialResponseStrategy, error) {
	enablePartialResponse := defEnable
	if val := values.Get(partialResponseParam); val != "" {
		var err error
		enablePartialResponse, err = strconv.ParseBool(val)
		if err != nil {
			return storepb.PartialResponseStrategy_WARN, errors.Wrapf(err, "'%s' parameter", partialResponseParam)
		}
	}

	// TODO
	if enablePartialResponse {
		return storepb.PartialResponseStrategy_WARN, nil
	}
	return storepb.PartialResponseStrategy_ABORT, nil
}

// WarningReporter allows to report warnings to frontend layer.
//
// Warning can include partial errors `partialResponse` is enabled. It occurs when only part of the results are ready and
// another is not available because of the failure.
// It is required to be thread-safe.
type warningReporter func(error)

// Options represents Thanos specific options for QueryAPI.
type Options struct {
	// Refer to storepb.Resolution
	Resolution *storepb.Resolution

	// Deduplicate controls if the retrieved data will be deduplicated along the replicaLabel.
	Deduplicate bool

	// Refer to storepb.PartialResponseStrategy
	PartialResponseStrategy storepb.PartialResponseStrategy
}

// NewOptionsFromForm parses options from form. All errors are because of BadData (InvalidArguments).
func NewOptionsFromForm(values url.Values, def Options) (Options, error) {
	opts := def

	maxSourceResolution := 0 * time.Second
	if val := values.Get(maxSourceResolutionParam); val != "" {
		var err error
		maxSourceResolution, err = parseDuration(val)
		if err != nil {
			return Options{}, errors.Wrapf(err, "'%s' parameter", maxSourceResolutionParam)
		}
	}

	if maxSourceResolution < 0 {
		return Options{}, errors.Errorf("negative '%s' is not accepted; try a positive integer", maxSourceResolutionParam)
	}

	if maxSourceResolution != 0 {
		opts.Resolution = &storepb.Resolution{
			Window: int64(maxSourceResolution / time.Millisecond),
			// TODO(bwplotka): Grab this from form as well.
			Strategy: storepb.Resolution_MAX,
		}
	}

	if val := values.Get(dedupParam); val != "" {
		var err error
		opts.Deduplicate, err = strconv.ParseBool(val)
		if err != nil {
			return Options{}, errors.Wrapf(err, "'%s' parameter", dedupParam)
		}
	}

	var err error
	opts.PartialResponseStrategy, err = NewPartialResponseStrategyFromForm(
		values,
		def.PartialResponseStrategy == storepb.PartialResponseStrategy_WARN,
	)
	if err != nil {
		return Options{}, err
	}
	return opts, nil
}

func (p Options) Encode(values url.Values) error {
	if p.Resolution != nil {
		values.Add(maxSourceResolutionParam, fmt.Sprintf("%d", p.Resolution.Window))
	}

	values.Add(dedupParam, strconv.FormatBool(p.Deduplicate))
	return EncodePartialResponseStrategy(p.PartialResponseStrategy, values)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
