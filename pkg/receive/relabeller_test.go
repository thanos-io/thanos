// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/extkingpin"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
)

func TestRelabeller_ConfigReload(t *testing.T) {
	t.Parallel()

	// Load the test data
	origRelabels, err := os.ReadFile("./testdata/relabel_config/good_relabels.yaml")
	testutil.Ok(t, err)
	invalidRelabels, err := os.ReadFile("./testdata/relabel_config/invalid_relabels.yaml")
	testutil.Ok(t, err)
	newRelabels, err := os.ReadFile("./testdata/relabel_config/good_relabels2.yaml")
	testutil.Ok(t, err)

	// Copy the original relabels file to a temporary directory.
	tempFilePath := path.Join(t.TempDir(), "relabels.yaml")
	testutil.Ok(t, os.WriteFile(tempFilePath, origRelabels, 0666))

	// Create a relabeller with the relabels file in the temporary directory.
	relabelFile, err := extkingpin.NewStaticPathContent(tempFilePath)
	testutil.Ok(t, err)
	relabeller, err := NewRelabeller(relabelFile, nil, log.NewLogfmtLogger(os.Stdout), 1*time.Second)
	testutil.Ok(t, err)

	config := relabeller.RelabelConfig()
	origRelabelsConfig, err := parseRelabel("./testdata/relabel_config/good_relabels.yaml")
	testutil.Ok(t, err)
	testutil.Equals(t, origRelabelsConfig, config)

	// Start the config reloader.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = relabeller.StartConfigReloader(ctx)
	testutil.Ok(t, err)

	// Rewrite the relabels file in temp with an invalid file, should not update the config.
	testutil.Ok(t, relabelFile.Rewrite(invalidRelabels))
	time.Sleep(1 * time.Second)
	testutil.Equals(t, origRelabelsConfig, relabeller.RelabelConfig())

	// Rewrite the relabels file in temp with a different valid file.
	testutil.Ok(t, relabelFile.Rewrite(newRelabels))
	newRelabelsConfig, err := parseRelabel("./testdata/relabel_config/good_relabels2.yaml")
	testutil.Ok(t, err)
	time.Sleep(1 * time.Second)
	testutil.Equals(t, newRelabelsConfig, relabeller.RelabelConfig())
}

func parseRelabel(path string) (RelabelConfig, error) {
	var relabelConfig RelabelConfig
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.UnmarshalStrict(file, &relabelConfig)
	if err != nil {
		return nil, err
	}
	return relabelConfig, nil
}

func TestRelabeller_CanReload(t *testing.T) {
	t.Parallel()

	validRelabelsPath, err := extkingpin.NewStaticPathContent("./testdata/relabel_config/good_relabels.yaml")
	testutil.Ok(t, err)
	emptyRelabelsPath := emptyPathFile{}

	type args struct {
		configFilePath fileContent
	}
	tests := []struct {
		name       string
		args       args
		wantReload bool
	}{
		{
			name:       "Nil config file path cannot be reloaded",
			args:       args{configFilePath: nil},
			wantReload: false,
		},
		{
			name:       "Empty config file path cannot be reloaded",
			args:       args{configFilePath: emptyRelabelsPath},
			wantReload: false,
		},
		{
			name:       "Valid config file path can be reloaded",
			args:       args{configFilePath: validRelabelsPath},
			wantReload: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configFile := tt.args.configFilePath
			relabeller, err := NewRelabeller(configFile, nil, log.NewLogfmtLogger(os.Stdout), 1*time.Second)
			testutil.Ok(t, err)
			if tt.wantReload {
				testutil.Assert(t, relabeller.CanReload())
			} else {
				testutil.Assert(t, !relabeller.CanReload())
			}
		})
	}
}
