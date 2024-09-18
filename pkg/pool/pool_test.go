// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/goleak"

	"github.com/efficientgo/core/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestBytesPool(t *testing.T) {
	chunkPool, err := NewBucketedPool[byte](10, 100, 2, 1000)
	testutil.Ok(t, err)

	testutil.Equals(t, []int{10, 20, 40, 80}, chunkPool.sizes)

	for i := 0; i < 10; i++ {
		b, err := chunkPool.Get(40)
		testutil.Ok(t, err)

		testutil.Equals(t, uint64(40), chunkPool.usedTotal)

		if i%2 == 0 {
			for j := 0; j < 6; j++ {
				*b = append(*b, []byte{'1', '2', '3', '4', '5'}...)
			}
		}
		chunkPool.Put(b)
	}

	for i := 0; i < 10; i++ {
		b, err := chunkPool.Get(19)
		testutil.Ok(t, err)
		chunkPool.Put(b)
	}

	// Outside of any bucket.
	b, err := chunkPool.Get(1000)
	testutil.Ok(t, err)
	chunkPool.Put(b)

	// Check size limitation.
	b1, err := chunkPool.Get(500)
	testutil.Ok(t, err)

	b2, err := chunkPool.Get(600)
	testutil.NotOk(t, err)
	testutil.Equals(t, ErrPoolExhausted, err)

	chunkPool.Put(b1)
	chunkPool.Put(b2)

	testutil.Equals(t, uint64(0), chunkPool.usedTotal)
}

func TestRacePutGet(t *testing.T) {
	chunkPool, err := NewBucketedPool[byte](3, 100, 2, 5000)
	testutil.Ok(t, err)

	s := sync.WaitGroup{}

	const goroutines = 100

	// Start multiple goroutines: they always Get and Put two byte slices
	// to which they write their contents and check if the data is still
	// there after writing it, before putting it back.
	errs := make(chan error, goroutines)
	stop := make(chan struct{})

	f := func(txt string, grow bool) {
		defer s.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c, err := chunkPool.Get(len(txt))
				if err != nil {
					errs <- errors.Wrapf(err, "goroutine %s", txt)
					return
				}

				*c = append(*c, txt...)
				if string(*c) != txt {
					errs <- errors.New("expected to get the data just written")
					return
				}
				if grow {
					*c = append(*c, txt...)
					*c = append(*c, txt...)
					if string(*c) != txt+txt+txt {
						errs <- errors.New("expected to get the data just written")
						return
					}
				}

				chunkPool.Put(c)
			}
		}
	}

	for i := 0; i < goroutines; i++ {
		s.Add(1)
		// make sure we start multiple goroutines with same len buf requirements, to hit same pools
		s := strings.Repeat(string(byte(i)), i%10)
		// some of the goroutines will append more elements to the provided slice
		grow := i%2 == 0
		go f(s, grow)
	}

	time.Sleep(1 * time.Second)
	close(stop)
	s.Wait()
	select {
	case err := <-errs:
		testutil.Ok(t, err)
	default:
	}
}
