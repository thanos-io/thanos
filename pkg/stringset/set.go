// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package stringset

import (
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

type Set interface {
	Has(string) bool
	HasAny([]string) bool
	// Count returns the number of elements in the set.
	// A value of -1 indicates infinite size and can be returned by a
	// set representing all possible string values.
	Count() int
}

type fixedSet struct {
	cuckoo *cuckoo.Filter
}

func (f fixedSet) HasAny(strings []string) bool {
	for _, s := range strings {
		if f.Has(s) {
			return true
		}
	}
	return false
}

func NewFromStrings(items ...string) Set {
	f := cuckoo.NewFilter(uint(len(items)))
	for _, label := range items {
		f.InsertUnique([]byte(label))
	}

	return &fixedSet{cuckoo: f}
}

func (f fixedSet) Has(s string) bool {
	return f.cuckoo.Lookup([]byte(s))
}

func (f fixedSet) Count() int {
	return int(f.cuckoo.Count())
}

type mutableSet struct {
	cuckoo *cuckoo.ScalableCuckooFilter
}

type MutableSet interface {
	Set
	Insert(string)
}

func New() MutableSet {
	return &mutableSet{
		cuckoo: cuckoo.NewScalableCuckooFilter(),
	}
}

func (e mutableSet) Insert(s string) {
	e.cuckoo.InsertUnique([]byte(s))
}

func (e mutableSet) Has(s string) bool {
	return e.cuckoo.Lookup([]byte(s))
}

func (e mutableSet) HasAny(strings []string) bool {
	for _, s := range strings {
		if e.Has(s) {
			return true
		}
	}
	return false
}

func (e mutableSet) Count() int {
	return int(e.cuckoo.Count())
}

type allStringsSet struct{}

func (e allStringsSet) HasAny(_ []string) bool {
	return true
}

func AllStrings() *allStringsSet {
	return &allStringsSet{}
}

func (e allStringsSet) Has(_ string) bool {
	return true
}

func (e allStringsSet) Count() int {
	return -1
}
