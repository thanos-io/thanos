package block

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/fileutil"

	"github.com/thanos-io/thanos/pkg/runutil"
)

type PartitionInfo struct {
	PartitionedGroupID uint32 `json:"partitionedGroupID"`
	PartitionCount     int    `json:"partitionCount"`
	PartitionID        int    `json:"partitionID"`
}

// WriteToDir writes the encoded partition info into <dir>/partition-info.json.
func (p PartitionInfo) WriteToDir(logger log.Logger, dir string) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, PartitionInfoFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := p.Write(f); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "close partition info")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return renameFile(logger, tmp, path)
}

// Write writes the given encoded partition info to writer.
func (p PartitionInfo) Write(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc.Encode(&p)
}

func renameFile(logger log.Logger, from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fdatasync(pdir); err != nil {
		runutil.CloseWithLogOnErr(logger, pdir, "close dir")
		return err
	}
	return pdir.Close()
}

// Read the block partition info from the given reader.
func ReadPartitionInfo(rc io.ReadCloser) (_ *PartitionInfo, err error) {
	defer runutil.ExhaustCloseWithErrCapture(&err, rc, "close partition info JSON")

	var p PartitionInfo
	if err = json.NewDecoder(rc).Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}
