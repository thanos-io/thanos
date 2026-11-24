// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import "capnproto.org/go/capnp/v3/exp/bufferpool"

// 24 buckets pool messages up to 8 MiB (bucket i holds 1<<i bytes) instead of 512 KiB,
// see https://github.com/capnproto/go-capnp/issues/664. BucketCount is read once on
// first use, so it is set here, before receive allocates from the pool.
func init() {
	bufferpool.Default.BucketCount = 24
}
