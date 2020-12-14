// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package swift

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestParseConfig(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
username: thanos
user_domain_name: userDomain
project_name: thanosProject
project_domain_name: projectDomain`)

	cfg, err := parseConfig(input)
	testutil.Ok(t, err)

	testutil.Equals(t, "http://identity.something.com/v3", cfg.AuthUrl)
	testutil.Equals(t, "thanos", cfg.Username)
	testutil.Equals(t, "userDomain", cfg.UserDomainName)
	testutil.Equals(t, "thanosProject", cfg.ProjectName)
	testutil.Equals(t, "projectDomain", cfg.ProjectDomainName)
}

func TestParseConfigFail(t *testing.T) {
	input := []byte(`auth_url: http://identity.something.com/v3
tenant_name: something`)

	_, err := parseConfig(input)
	// Must result in unmarshal error as there's no `tenant_name` in SwiftConfig.
	testutil.NotOk(t, err)
}
