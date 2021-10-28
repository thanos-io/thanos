package remotewrite

import (
	"path/filepath"
)

// SubDirectory returns the subdirectory within a Storage directory used for
// the Prometheus WAL.
func SubDirectory(base string) string {
	return filepath.Join(base, "wal")
}
