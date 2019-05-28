package flagutil

import (
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
)

type PathOrContent struct {
	fileFlagName    string
	contentFlagName string

	required bool
	path     *string
	content  *string
}

// Content returns content of the file. Flag that specifies path has priority.
// It returns error if the content is empty and required flag is set to true.
func (p *PathOrContent) Content() ([]byte, error) {
	if len(*p.path) > 0 && len(*p.content) > 0 {
		return nil, errors.Errorf("Both %s and %s flags set.", p.fileFlagName, p.contentFlagName)
	}

	var content []byte
	if len(*p.path) > 0 {
		c, err := ioutil.ReadFile(*p.path)
		if err != nil {
			return nil, errors.Wrapf(err, "loading YAML file %s for %s", *p.path, p.fileFlagName)
		}
		content = c
	} else {
		content = []byte(*p.content)
	}

	if len(content) == 0 && p.required {
		return nil, errors.Errorf("flag %s or %s is required for running this command and content cannot be empty.", p.fileFlagName, p.contentFlagName)
	}

	return content, nil
}

// NewPathOrContentFlag returns single flag wrapper that returns content from file flag if specified, if not from content flag directly.
func NewPathOrContentFlag(fileFlag *kingpin.FlagClause,contentFlag *kingpin.FlagClause,	required bool) *PathOrContent {
	return &PathOrContent{
		fileFlagName:    fileFlag.Model().Name,
		contentFlagName: contentFlag.Model().Name,
		required:        required,

		path:    fileFlag.String(),
		content: contentFlag.String(),
	}
}