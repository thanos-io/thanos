package storecache

// StorageCache is a wrapper around typical Get()/Set() operations
// of a cache. Some might be a no-op on certain implementations.
type StorageCache interface {
	Get(key interface{}) (val interface{}, ok bool)
	Add(key interface{}, val interface{})
	RemoveOldest() (key interface{}, val interface{}, ok bool)
	Purge()
	KeyData() bool // True if it retains exact information about keys.
}
