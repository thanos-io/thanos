// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	var emptyCfgErr EmptyConfigurationError

	for _, tc := range []struct {
		name string
		cfg  interface{}
		err  interface{}
	}{
		{
			name: "<nil> config",
			cfg:  nil,
			err:  emptyCfgErr,
		},
		{
			name: "empty config",
			cfg:  []HashringConfig{},
			err:  emptyCfgErr,
		},
		{
			name: "unparsable config",
			cfg:  struct{}{},
			err:  json.UnsupportedTypeError{},
		},
		{
			name: "valid config",
			cfg: []HashringConfig{
				{
					Endpoints: []string{"node1"},
				},
			},
			err: nil, // means it's valid.
		},
	} {
		var content []byte
		var err error
		if content, err = json.Marshal(tc.cfg); err != nil {
			t.Error(err)
		}

		tmpfile, err := ioutil.TempFile("", "configwatcher_test.*.json")
		if err != nil {
			t.Fatalf("case %q: unexpectedly failed creating the temp file: %v", tc.name, err)
		}
		defer os.Remove(tmpfile.Name())

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatalf("case %q: unexpectedly failed writing to the temp file: %v", tc.name, err)
		}

		if err := tmpfile.Close(); err != nil {
			t.Fatalf("case %q: unexpectedly failed closing the temp file: %v", tc.name, err)
		}

		cw, err := NewConfigWatcher(nil, nil, tmpfile.Name(), 1)
		if err != nil {
			t.Fatalf("case %q: unexpectedly failed creating config watcher: %v", tc.name, err)
		}

		if err := cw.ValidateConfig(); err != nil && !errors.As(err, &tc.err) {
			t.Errorf("case %q: got unexpected error: %v", tc.name, err)
			continue
		}
	}
}
