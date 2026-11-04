// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

func TestStepAlignUsesRequestTimezone(t *testing.T) {
	location := time.FixedZone("Asia/Shanghai", 8*60*60)
	dayMillis := int64((24 * time.Hour) / time.Millisecond)
	input := &ThanosQueryRangeRequest{
		Start:                 2*dayMillis + 10*60*60*1000,
		End:                   4*dayMillis + 10*60*60*1000,
		Step:                  dayMillis,
		StepAlignmentLocation: location,
	}

	var result *ThanosQueryRangeRequest
	handler := queryrange.StepAlignMiddleware.Wrap(queryrange.HandlerFunc(func(_ context.Context, request queryrange.Request) (queryrange.Response, error) {
		result = request.(*ThanosQueryRangeRequest)
		return nil, nil
	}))

	_, err := handler.Do(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, int64(dayMillis+16*60*60*1000), result.Start)
	require.Equal(t, int64(3*dayMillis+16*60*60*1000), result.End)
}
