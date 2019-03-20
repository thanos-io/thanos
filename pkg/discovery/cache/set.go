package cache

import (
	"sync"
)

// Cache is a store for target groups. It provides thread safe updates and a way for obtaining all addresses from
// the stored target groups.
type Set struct {
	data map[string]bool
	sync.Mutex
}

// New returns a new empty Set.
func NewSet() *Set {
	return &Set{
		data: make(map[string]bool),
	}
}

// Update stores the targets for the given groups.
// Note: targets for a group are replaced entirely on update. If a group with no target is given this is equivalent to
// deleting all the targets for this group.
func (c *Set) Add(address string) {
	c.Lock()
	defer c.Unlock()

	c.data[address] = true
}

func (c *Set) Delete(address string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.data[address]; ok {
		delete(c.data, address)
	}
}

func (c *Set) Clear() {
	c.Lock()
	defer c.Unlock()

	c.data = make(map[string]bool)
}

func (c *Set) ReplaceBy(addresses []string) {
	c.Lock()
	defer c.Unlock()

	c.data = make(map[string]bool)
	for _, addr := range addresses {
		c.data[addr] = true
	}
}

// Addresses returns all the addresses from all target groups present in the Cache.
func (c *Set) Addresses() []string {
	c.Lock()
	defer c.Unlock()

	var addresses []string
	for address, _ := range c.data {
		addresses = append(addresses, address)
	}
	return addresses
}
