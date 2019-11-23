package limit

import (
	"fmt"
	"math/bits"
	"os"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

const (
	envQueryPipeLimit  = "THANOS_LIMIT_QUERY_PIPE"
	envQueryTotalLimit = "THANOS_LIMIT_QUERY_TOTAL"

	// TODO(ppanyukov): do we really need this with other limits?
	// This limit does not seem to achieve much some pathological queries either.
	envPromqlMaxSamples = "THANOS_LIMIT_PROMQL_MAX_SAMPLES"
)

const (
	// platform-specific size of int
	maxInt int = 1<<(bits.UintSize-1) - 1 // 1<<31 - 1 or 1<<63 - 1
)

var (
	// queryPipeLimit is the number of bytes querier can receive from one given source.
	// zero or negative == no limit.
	queryPipeLimit int64 = getLimitFromEnvVar(envQueryPipeLimit, 0)

	// queryTotalLimit is the total number of bytes querier can receive from all sources overall.
	// zero or negative == no limit.
	queryTotalLimit int64 = getLimitFromEnvVar(envQueryTotalLimit, 0)

	// See PromqlMaxSamples() func.
	promqlMaxSamples int = func() int {
		var v int64 = getLimitFromEnvVar(envPromqlMaxSamples, int64(maxInt))

		if v > int64(maxInt) {
			return maxInt
		}

		return int(v)
	}()
)

// PromqlMaxSamples is the limit on max samples PromQL can handle.
// By default it's the original Thanos math.MaxInt32. In Prometheus itself
// the default is 50M == 50M * 16 bytes sample size == 800MB RAM.
// This value should be used to initialise PromQL engine like so:
//
//		engine = promql.NewEngine(
//			promql.EngineOpts{
//				MaxSamples: limit.PromqlMaxSamples(),
//		)
//
// For details and application of this limit see these:
//	- blog post: https://www.robustperception.io/limiting-promql-resource-usage
//  - PR: https://github.com/prometheus/prometheus/pull/4513/files
//
func PromqlMaxSamples() int {
	return promqlMaxSamples
}

func LogInfo(logger log.Logger) {
	_ = level.Debug(logger).Log(envQueryPipeLimit, byteCountToHuman(queryPipeLimit), envQueryTotalLimit, byteCountToHuman(queryTotalLimit))
	_ = level.Debug(logger).Log(envPromqlMaxSamples, promqlLimitToHuman(PromqlMaxSamples()))
}

func CheckQueryPipeLimit(queryPipeSize int64) error {
	if queryPipeLimit <= 0 {
		return nil
	}

	if queryPipeSize > queryPipeLimit {
		err := errors.Errorf("%s limit %s violated (got %s)",
			envQueryPipeLimit,
			byteCountToHuman(queryPipeLimit),
			byteCountToHuman(queryPipeSize))
		return err
	}
	return nil
}

func CheckQueryTotalLimit(queryTotalSize int64) error {
	if queryTotalLimit <= 0 {
		return nil
	}

	if queryTotalSize > queryTotalLimit {
		err := errors.Errorf("%s limit %s violated (got %s)",
			envQueryTotalLimit,
			byteCountToHuman(queryTotalLimit),
			byteCountToHuman(queryTotalSize))
		return err
	}
	return nil
}

func getLimitFromEnvVar(envVarName string, defaultValue int64) int64 {
	var (
		parsedLimit int64
		err         error
	)

	if qpl := os.Getenv(envVarName); qpl != "" {
		parsedLimit, err = strconv.ParseInt(qpl, 10, 0)
		if err != nil {
			parsedLimit = defaultValue
		}
	}

	if parsedLimit <= 0 {
		parsedLimit = defaultValue
	}

	return parsedLimit
}

func byteCountToHuman(n int64) string {
	const (
		kb = 1000
		mb = 1000 * kb
		gb = 1000 * mb
		tb = 1000 * gb
	)

	switch {
	case n >= tb:
		return fmt.Sprintf("%.2fTB", float64(n)/float64(tb))
	case n >= gb:
		return fmt.Sprintf("%.2fGB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2fMB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2fKB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

func promqlLimitToHuman(n int) string {
	const (
		sampleSize = 16
	)

	const (
		kb = 1000
		mb = 1000 * kb
		gb = 1000 * mb
		tb = 1000 * gb
	)

	var samplesInBytes = byteCountToHuman(int64(n) * int64(sampleSize))

	switch {
	case n >= tb:
		return fmt.Sprintf("%.2fT samples == %s", float64(n)/float64(tb), samplesInBytes)
	case n >= gb:
		return fmt.Sprintf("%.2fG samples == %s", float64(n)/float64(gb), samplesInBytes)
	case n >= mb:
		return fmt.Sprintf("%.2fM samples == %s", float64(n)/float64(mb), samplesInBytes)
	case n >= kb:
		return fmt.Sprintf("%.2fK samples == %s", float64(n)/float64(kb), samplesInBytes)
	default:
		return fmt.Sprintf("%d samples == %s", n, samplesInBytes)
	}
}
