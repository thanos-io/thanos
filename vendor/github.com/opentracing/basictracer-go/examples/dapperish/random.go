package dapperish

import (
	"math/rand"
	"sync"
	"time"
)

var (
	seededIDGen  = rand.New(rand.NewSource(time.Now().UnixNano()))
	seededIDLock sync.Mutex
)

// Generate [UnixNano-seeded] pseudorandom numbers.
func randomID() int64 {
	// The golang rand generators are *not* intrinsically thread-safe.
	seededIDLock.Lock()
	defer seededIDLock.Unlock()
	return seededIDGen.Int63()
}
