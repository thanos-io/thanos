package indexcache

// IndexCache is a provider of an index cache with a different underlying implementation.
type IndexCache interface {
	WriteIndexCache(indexFn string, fn string) error
	ReadIndexCache(fn string) error
}
