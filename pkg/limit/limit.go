package limit

import (
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

const (
	envQueryPipeLimit  = "THANOS_LIMIT_QUERY_PIPE"
	envQueryTotalLimit = "THANOS_LIMIT_QUERY_TOTAL"
)

var (
	// queryPipeLimit is the number of bytes querier can receive from one given source.
	// zero or negative == no limit
	queryPipeLimit int64 = getLimitFromEnvVar(envQueryPipeLimit)

	// queryTotalLimit is the total number of bytes querier can receive from all sources overall.
	// zero or negative == no limit
	queryTotalLimit int64 = getLimitFromEnvVar(envQueryTotalLimit)
)

func CheckQueryPipeLimit(queryPipeSize int64) error {
	if queryPipeLimit <= 0 {
		return nil
	}

	if queryPipeSize > queryPipeLimit {
		err := errors.Errorf("%s limit %s violated (got %s)",
			envQueryPipeLimit,
			byteCountToHuman(queryPipeLimit),
			byteCountToHuman(queryPipeSize))
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
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
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		return err
	}
	return nil
}

func getLimitFromEnvVar(envVarName string) int64 {
	var (
		parsedLimit int64
		err         error
	)

	if qpl := os.Getenv(envVarName); qpl != "" {
		parsedLimit, err = strconv.ParseInt(qpl, 10, 0)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "WARNING: Cannot parse %s as int: %v. Setting limit to 0 (off).\n", envVarName, err)
			parsedLimit = 0
		}
	}

	if parsedLimit <= 0 {
		parsedLimit = 0
	}

	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", envVarName, byteCountToHuman(parsedLimit))
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
