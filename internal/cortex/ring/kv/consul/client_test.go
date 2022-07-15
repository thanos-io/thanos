// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package consul

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/ring/kv/codec"
)

func writeValuesToKV(t *testing.T, client *Client, key string, start, end int, sleep time.Duration) <-chan struct{} {
	t.Helper()

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for i := start; i <= end; i++ {
			t.Log("ts", time.Now(), "msg", "writing value", "val", i)
			_, _ = client.kv.Put(&consul.KVPair{Key: key, Value: []byte(fmt.Sprintf("%d", i))}, nil)
			time.Sleep(sleep)
		}
	}()
	return ch
}

func TestWatchKeyWithRateLimit(t *testing.T) {
	c, closer := NewInMemoryClientWithConfig(codec.String{}, Config{
		WatchKeyRateLimit: 5.0,
		WatchKeyBurstSize: 1,
	}, testLogger{}, prometheus.NewPedanticRegistry())
	t.Cleanup(func() {
		assert.NoError(t, closer.Close())
	})

	const key = "test"
	const max = 100

	ch := writeValuesToKV(t, c, key, 0, max, 10*time.Millisecond)

	observed := observeValueForSomeTime(t, c, key, 1200*time.Millisecond) // little over 1 second

	// wait until updater finishes
	<-ch

	if testing.Verbose() {
		t.Log(observed)
	}
	// Let's see how many updates we have observed. Given the rate limit and our observing time, it should be 6
	// We should also have seen one of the later values, as we're observing for longer than a second, so rate limit should allow
	// us to see it.
	if len(observed) < 5 || len(observed) > 10 {
		t.Error("Expected ~6 observed values, got", observed)
	}
	last := observed[len(observed)-1]
	n, _ := strconv.Atoi(last)
	if n < max/2 {
		t.Error("Expected to see high last observed value, got", observed)
	}
}

func TestWatchKeyNoRateLimit(t *testing.T) {
	c, closer := NewInMemoryClientWithConfig(codec.String{}, Config{
		WatchKeyRateLimit: 0,
	}, testLogger{}, prometheus.NewPedanticRegistry())
	t.Cleanup(func() {
		assert.NoError(t, closer.Close())
	})

	const key = "test"
	const max = 100

	ch := writeValuesToKV(t, c, key, 0, max, time.Millisecond)
	observed := observeValueForSomeTime(t, c, key, 500*time.Millisecond)

	// wait until updater finishes
	<-ch

	// With no limit, we should see most written values (we can lose some values if watching
	// code is busy while multiple new values are written)
	if len(observed) < 3*max/4 {
		t.Error("Expected at least 3/4 of all values, got", observed)
	}
}

func TestReset(t *testing.T) {
	c, closer := NewInMemoryClient(codec.String{}, testLogger{}, prometheus.NewPedanticRegistry())
	t.Cleanup(func() {
		assert.NoError(t, closer.Close())
	})

	const key = "test"
	const max = 5

	ch := make(chan error)
	go func() {
		defer close(ch)
		for i := 0; i <= max; i++ {
			t.Log("ts", time.Now(), "msg", "writing value", "val", i)
			_, _ = c.kv.Put(&consul.KVPair{Key: key, Value: []byte(fmt.Sprintf("%d", i))}, nil)
			if i == 1 {
				c.kv.(*mockKV).ResetIndex()
			}
			if i == 2 {
				c.kv.(*mockKV).ResetIndexForKey(key)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	observed := observeValueForSomeTime(t, c, key, 25*max*time.Millisecond)

	// wait until updater finishes
	<-ch

	// Let's see how many updates we have observed. Given the rate limit and our observing time, we should see all numeric values
	if testing.Verbose() {
		t.Log(observed)
	}
	if len(observed) < max {
		t.Error("Expected all values, got", observed)
	} else if observed[len(observed)-1] != fmt.Sprintf("%d", max) {
		t.Error("Expected to see last written value, got", observed)
	}
}

func observeValueForSomeTime(t *testing.T, client *Client, key string, timeout time.Duration) []string {
	t.Helper()

	observed := []string(nil)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client.WatchKey(ctx, key, func(i interface{}) bool {
		s, ok := i.(string)
		if !ok {
			return false
		}
		t.Log("ts", time.Now(), "msg", "observed value", "val", s)
		observed = append(observed, s)
		return true
	})
	return observed
}

func TestWatchKeyWithNoStartValue(t *testing.T) {
	c, closer := NewInMemoryClient(codec.String{}, testLogger{}, prometheus.NewPedanticRegistry())
	t.Cleanup(func() {
		assert.NoError(t, closer.Close())
	})

	const key = "test"

	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := c.kv.Put(&consul.KVPair{Key: key, Value: []byte("start")}, nil)
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		_, err = c.kv.Put(&consul.KVPair{Key: key, Value: []byte("end")}, nil)
		require.NoError(t, err)
	}()

	ctx, fn := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer fn()

	reported := 0
	c.WatchKey(ctx, key, func(i interface{}) bool {
		reported++
		return reported != 2
	})

	// we should see both start and end values.
	require.Equal(t, 2, reported)
}

type testLogger struct {
}

func (l testLogger) Log(keyvals ...interface{}) error {
	return nil
}
