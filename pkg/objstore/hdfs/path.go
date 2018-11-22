package hdfs

import (
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

// hdfsPath is a rooted path in HDFS. It is guaranteed to start with a slash and
// not to end with a slash, except for the root path "/". Create an hdfsPath
// using the buildPath method.
// https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/model.html
type hdfsPath string

const (
	invalidHdfsPath = hdfsPath("")
	hdfsRootPath    = hdfsPath("/")
)

func buildPath(elem ...string) (hdfsPath, error) {
	// Path elements MUST NOT be in {"", ".", "..", "/"}.
	// Path elements MUST NOT contain the characters {'/', ':'}.

	switch len(elem) {
	case 0:
		return "", errors.New("empty path")

	case 1:
		if _, err := checkRootPath(elem[0]); err != nil {
			return "", err
		}
		return hdfsPath(elem[0]), nil
	}

	return buildRootedPath(elem[0], elem[1:])
}

func buildRootedPath(root string, elem []string) (hdfsPath, error) {
	isRoot, err := checkRootPath(root)
	if err != nil {
		return invalidHdfsPath, err
	}

	for _, el := range elem {
		if err := checkPath(el); err != nil {
			return invalidHdfsPath, err
		}
	}

	if isRoot {
		return hdfsPath("/" + strings.Join(elem, "/")), nil
	}

	return hdfsPath(root + "/" + strings.Join(elem, "/")), nil
}

func checkRootPath(path string) (root bool, err error) {
	if path == "/" {
		return true, nil
	}

	if !strings.HasPrefix(path, "/") {
		return false, errors.Errorf("root paths must start with a slash: %q", path)
	}

	return false, checkPath(path[1:])
}

func checkPath(path string) error {
	type state int
	const (
		slashSeen state = iota
		dotSeen
		dotDotSeen
		inElem
	)
	s := slashSeen

	for _, ch := range path {
		switch {
		case ch == '/':
			switch s {
			case slashSeen:
				return errors.Errorf("empty path element: %q", path)
			case dotSeen, dotDotSeen:
				return errors.Errorf("invalid path element: %q", path)
			}
			s = slashSeen

		case ch == '.':
			switch s {
			case slashSeen:
				s = dotSeen
			case dotSeen:
				s = dotDotSeen
			case dotDotSeen:
				s = inElem
			}

		case unicode.IsControl(ch), ch == ':', ch == unicode.ReplacementChar:
			return errors.Errorf("illegal character in path: %q", path)

		default:
			s = inElem
		}
	}

	switch s {
	case slashSeen:
		return errors.Errorf("empty path element: %q", path)
	case dotSeen, dotDotSeen:
		return errors.Errorf("invalid path element: %q", path)
	}

	return nil
}

func (p hdfsPath) join(elem ...string) (hdfsPath, error) {
	return buildRootedPath(string(p), elem)
}

func (p hdfsPath) parent() (hdfsPath, bool) {
	splitPoint := strings.LastIndex(string(p), "/")
	if splitPoint < 0 {
		return invalidHdfsPath, false
	}
	if splitPoint == 0 {
		return hdfsRootPath, len(p) > 1
	}

	path := p[:splitPoint]
	if _, err := checkRootPath(string(path)); err != nil {
		return invalidHdfsPath, false
	}

	return path, true
}

func (p hdfsPath) isParentOf(other hdfsPath) bool {
	if p == invalidHdfsPath {
		return false
	}
	if p == hdfsRootPath && other != hdfsRootPath {
		return true
	}

	if len(other) >= len(p)+1 && other[len(p)] == '/' && other[0:len(p)] == p {
		_, err := checkRootPath(string(p))
		return err == nil
	}

	return false
}
