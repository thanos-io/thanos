package hdfs

import (
	"fmt"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

var validParts = []string{
	"...",
	"a", "ab",
	".a", ".ab",
	"a.", "a.b",
	"..a", "..ab",
	"a..", "a..b",
}

func TestBuildHdfsPath(t *testing.T) {
	test := func(expected string, parts ...string) {
		t.Run(fmt.Sprintf("buildPath(%q)", parts), func(t *testing.T) {
			path, err := buildPath(parts...)
			testutil.Ok(t, err)
			testutil.Equals(t, expected, string(path))
		})
	}

	var buildSpec = [][]string{
		[]string{"/"}, []string{"/"},
		[]string{"/foo"}, []string{"/foo"},
		[]string{"/foo", "bar"}, []string{"/foo/bar"},
	}

	for _, part := range validParts {
		test("/"+part, "/", part)
		test("/"+part, "/"+part)
	}

	for i := 0; i < len(buildSpec); i += 2 {
		test(buildSpec[i+1][0], buildSpec[i]...)
	}
}

func TestBuildBadHdfsPath(t *testing.T) {
	test := func(parts ...string) {
		t.Run(fmt.Sprintf("buildPath(%q)", parts), func(t *testing.T) {
			path, err := buildPath(parts...)
			testutil.NotOk(t, err)
			testutil.Equals(t, "", string(path))
		})
	}

	test([]string(nil)...)
	test([]string{}...)
	test("/", "/")
	test("//")

	for _, invalidPart := range []string{
		".", "..", ":", "x:", ":x", "x:y",
	} {
		test(invalidPart)
		test("/" + invalidPart)
		test("/", invalidPart)
		test(invalidPart, "/")
		test(invalidPart + "/")
		for _, validPart := range validParts {
			for _, parts := range [][]string{{invalidPart, validPart}, {validPart, invalidPart}} {
				test(parts[0], parts[1])
				test("/", parts[0], parts[1])
				test("/"+parts[0], parts[1])
				test(parts[0] + "/" + parts[1])
				test("/" + parts[0] + "/" + parts[1])
			}
		}
	}

	for _, validPart := range validParts {
		test(validPart)
		test(validPart + "/")
		test(validPart, "/")
	}
}

var pathsWithParents = map[string]string{
	"/foo":        "/",
	"/foo/":       "/foo",
	"/foo/\\":     "/foo",
	"/foo\\/":     "/foo\\",
	"/foo/bar":    "/foo",
	"/foo/\\bar":  "/foo",
	"/foo\\/bar":  "/foo\\",
	"/foo/bar/":   "/foo/bar",
	"/foo/\\bar/": "/foo/\\bar",
	"/foo\\/bar/": "/foo\\/bar",
}

func TestHdfsParentPath(t *testing.T) {
	test := func(path, parent hdfsPath, ok bool) {
		t.Run(fmt.Sprintf("%q.parent()", path), func(t *testing.T) {
			actParent, actOk := path.parent()
			testutil.Equals(t, parent, actParent)
			testutil.Equals(t, ok, actOk)
		})
	}

	for path, parent := range map[hdfsPath]hdfsPath{
		invalidHdfsPath: invalidHdfsPath,
		hdfsRootPath:    hdfsRootPath,
		hdfsPath("foo"): invalidHdfsPath,
	} {
		test(path, parent, false)
	}

	for path, parent := range pathsWithParents {
		test(hdfsPath(path), hdfsPath(parent), true)
		test(hdfsPath(path[1:]), invalidHdfsPath, false)
	}
}

func TestHdfsIsParentOfPath(t *testing.T) {
	test := func(parent, other hdfsPath, isParent bool) {
		t.Run(fmt.Sprintf("%q.isParentOf(%q)", parent, other), func(t *testing.T) {
			actIsParent := parent.isParentOf(other)
			testutil.Equals(t, isParent, actIsParent)
		})
	}

	for path, parent := range pathsWithParents {
		test(hdfsPath(parent), hdfsPath(path), true)
		test(hdfsPath(parent[1:]), hdfsPath(path[1:]), false)
		test(hdfsPath(path), hdfsPath(parent), false)
		test(hdfsPath(path), hdfsPath(path), false)
		test(hdfsPath(parent), hdfsPath(parent), false)
	}
}
