package cache

import (
	"sync"
)

// Cache is a store for target groups. It provides thread safe updates and a way for obtaining all addresses from
// the stored target groups.
type KVStore struct {
	set map[string]bool
	sync.Mutex
}

// New returns a new empty Cache.
func NewKVStore() *KVStore {
	return &KVStore{
		set: make(map[string]bool),
	}
}

// Update stores the targets for the given groups.
// Note: targets for a group are replaced entirely on update. If a group with no target is given this is equivalent to
// deleting all the targets for this group.
func (c *KVStore) Add(address string) {
	c.Lock()
	defer c.Unlock()

	c.set[address] = true
}

func (c *KVStore) Delete(address string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.set[address]; ok {
		delete(c.set, address)
	}
}

// TODO: replace it, don't just remove all
func (c *KVStore) Clear() {
	c.Lock()
	defer c.Unlock()

	c.set = make(map[string]bool)

}

// Addresses returns all the addresses from all target groups present in the Cache.
func (c *KVStore) Addresses() []string {
	c.Lock()
	defer c.Unlock()

	var addresses []string
	for address, _ := range c.set {
		addresses = append(addresses, address)
	}
	return addresses
}
