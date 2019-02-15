package pool

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestBytesPool(t *testing.T) {
	chunkPool, err := NewBytesPool(10, 100, 2, 1000)
	testutil.Ok(t, err)

	// Inject alloc counter.
	allocs := uint64(0)
	wrapped := chunkPool.new
	chunkPool.new = func(sz int) []byte {
		allocs++
		return wrapped(sz)
	}
	testutil.Equals(t, []int{10, 20, 40, 80}, chunkPool.sizes)

	for i := 0; i < 10; i++ {
		b, err := chunkPool.Get(40)
		testutil.Ok(t, err)

		testutil.Equals(t, uint64(40), chunkPool.usedTotal)

		if i%2 == 0 {
			for j := 0; j < 6; j++ {
				b = append(b, []byte{'1', '2', '3', '4', '5'}...)
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
	testutil.Equals(t, uint64(4), allocs)
}
