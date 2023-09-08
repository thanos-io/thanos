// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cachekey

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	ErrInvalidBucketCacheKeyFormat = errors.New("key has invalid format")
	ErrInvalidBucketCacheKeyVerb   = errors.New("key has invalid verb")
	ErrParseKeyInt                 = errors.New("failed to parse integer in key")
)

// VerbType is the type of operation whose result has been stored in the caching bucket's cache.
type VerbType string

const (
	ExistsVerb        VerbType = "exists"
	ContentVerb       VerbType = "content"
	IterVerb          VerbType = "iter"
	IterRecursiveVerb VerbType = "iter-recursive"
	AttributesVerb    VerbType = "attrs"
	SubrangeVerb      VerbType = "subrange"
)

type BucketCacheKey struct {
	Verb  VerbType
	Name  string
	Start int64
	End   int64
}

// String returns the string representation of BucketCacheKey.
func (ck BucketCacheKey) String() string {
	if ck.Start == 0 && ck.End == 0 {
		return string(ck.Verb) + ":" + ck.Name
	}

	return strings.Join([]string{string(ck.Verb), ck.Name, strconv.FormatInt(ck.Start, 10), strconv.FormatInt(ck.End, 10)}, ":")
}

// IsValidVerb checks if the VerbType matches the predefined verbs.
func IsValidVerb(v VerbType) bool {
	switch v {
	case
		ExistsVerb,
		ContentVerb,
		IterVerb,
		IterRecursiveVerb,
		AttributesVerb,
		SubrangeVerb:
		return true
	}
	return false
}

// ParseBucketCacheKey parses a string and returns BucketCacheKey.
func ParseBucketCacheKey(key string) (BucketCacheKey, error) {
	ck := BucketCacheKey{}
	slice := strings.Split(key, ":")
	if len(slice) < 2 {
		return ck, ErrInvalidBucketCacheKeyFormat
	}

	verb := VerbType(slice[0])
	if !IsValidVerb(verb) {
		return BucketCacheKey{}, ErrInvalidBucketCacheKeyVerb
	}

	if verb == SubrangeVerb {
		if len(slice) != 4 {
			return BucketCacheKey{}, ErrInvalidBucketCacheKeyFormat
		}

		start, err := strconv.ParseInt(slice[2], 10, 64)
		if err != nil {
			return BucketCacheKey{}, ErrParseKeyInt
		}

		end, err := strconv.ParseInt(slice[3], 10, 64)
		if err != nil {
			return BucketCacheKey{}, ErrParseKeyInt
		}

		ck.Start = start
		ck.End = end
	} else {
		if len(slice) != 2 {
			return BucketCacheKey{}, ErrInvalidBucketCacheKeyFormat
		}
	}

	ck.Verb = verb
	ck.Name = slice[1]
	return ck, nil
}
