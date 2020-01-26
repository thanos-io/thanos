// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBytesPool(t *testing.T) {
	chunkPool, err := NewBucketedBytesPool(10, 100, 2, 1000)
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
	chunkPool, err := NewBucketedBytesPool(3, 100, 2, 5000)
	testutil.Ok(t, err)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	s := sync.WaitGroup{}

	// Start two goroutines: they always Get and Put two byte slices
	// to which they write 'foo' / 'barbazbaz' and check if the data is still
	// there after writing it, before putting it back.
	errs := make(chan error, 2)
	stop := make(chan bool, 2)

	f := func(txt string) {
		for {
			select {
			case <-stop:
				s.Done()
				return
			default:
				c, err := chunkPool.Get(3)
				if err != nil {
					errs <- errors.Wrapf(err, "goroutine %s", txt)
					s.Done()
					return
				}

				buf := bytes.NewBuffer(*c)

				_, err = fmt.Fprintf(buf, "%s", txt)
				if err != nil {
					errs <- errors.Wrapf(err, "goroutine %s", txt)
					s.Done()
					return
				}

				if buf.String() != txt {
					errs <- errors.New("expected to get the data just written")
					s.Done()
					return
				}

				b := buf.Bytes()
				chunkPool.Put(&b)
			}
		}
	}

	s.Add(2)
	go f("foo")
	go f("barbazbaz")

	time.Sleep(5 * time.Second)
	stop <- true
	stop <- true

	s.Wait()
	select {
	case err := <-errs:
		testutil.Ok(t, err)
	default:
	}
}
