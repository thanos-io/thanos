// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache

func Flush(c Cache) {
	b := c.(*backgroundCache)
	close(b.bgWrites)
	b.wg.Wait()
}
