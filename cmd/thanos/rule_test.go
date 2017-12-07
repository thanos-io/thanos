package main

import (
	"testing"

	"io/ioutil"
	"os"
	"path"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestGetRulesFiles(t *testing.T) {
	// Create testdata
	tmpDir, err := ioutil.TempDir(os.TempDir(), "thanos-ruler")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDir)

	// Hidden files should be ignored.
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, ".hidden_file.yml"), nil, os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "..hidden_file2.yml"), nil, os.ModePerm))

	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "ok_file.yml"), nil, os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "ok_file2.yaml"), nil, os.ModePerm))

	// Files with incorrect extensions.
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "wrong_file.yaml2"), nil, os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "wrong_file"), nil, os.ModePerm))

	// Directory.
	testutil.Ok(t, os.Mkdir(path.Join(tmpDir, "dir.yaml"), os.ModePerm)) // To confuse even more.

	// Recursive file with correct extension.
	testutil.Ok(t, ioutil.WriteFile(path.Join(tmpDir, "dir.yaml", "target_file.yml"), nil, os.ModePerm))

	// Correct symlinks.
	testutil.Ok(t, os.Symlink(path.Join("dir.yaml", "target_file.yml"), path.Join(tmpDir, "sym_file.yml")))
	testutil.Ok(t, os.Symlink(path.Join(tmpDir, "dir.yaml", "target_file.yml"), path.Join(tmpDir, "sym_abs_file.yml")))

	// Problematic symlink that targets directory.
	testutil.Ok(t, os.Symlink(path.Join("dir.yaml"), path.Join(tmpDir, "sym_dir")))

	files, err := getRulesFiles(log.NewNopLogger(), tmpDir)
	testutil.Ok(t, err)
	testutil.Equals(t, []string{
		path.Join(tmpDir, "dir.yaml", "target_file.yml"),
		path.Join(tmpDir, "ok_file.yml"),
		path.Join(tmpDir, "ok_file2.yaml"),
		path.Join(tmpDir, "sym_abs_file.yml"),
		path.Join(tmpDir, "sym_file.yml"),
	}, files)
}
