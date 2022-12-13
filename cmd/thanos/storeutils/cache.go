package storeutils

import lru "github.com/hashicorp/golang-lru"

type GobCache struct {
	lru  *lru.Cache
	file string
}
