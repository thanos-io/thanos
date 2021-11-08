// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package memcache

import (
	"bufio"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestGoodClusterConfigs(t *testing.T) {
	resolver := memcachedAutoDiscovery{}
	testCases := []struct {
		content string
		config  clusterConfig
	}{
		{"CONFIG cluster 0 23\r\n100\r\ndns-1|ip-1|11211\r\nEND\r\n",
			clusterConfig{nodes: []node{{dns: "dns-1", ip: "ip-1", port: 11211}}, version: 100},
		},
		{"CONFIG cluster 0 37\r\n0\r\ndns-1|ip-1|11211 dns-2|ip-2|8080\r\nEND\r\n",
			clusterConfig{nodes: []node{{dns: "dns-1", ip: "ip-1", port: 11211}, {dns: "dns-2", ip: "ip-2", port: 8080}}, version: 0},
		},
	}

	for _, testCase := range testCases {
		reader := bufio.NewReader(strings.NewReader(testCase.content))

		config, err := resolver.parseConfig(reader)

		testutil.Ok(t, err)
		testutil.Equals(t, testCase.config, *config)
	}
}

func TestBadClusterConfigs(t *testing.T) {
	resolver := memcachedAutoDiscovery{}
	testCases := []struct {
		content     string
		expectedErr error
	}{
		{"",
			errors.New("failed to read config metadata: EOF"),
		},
		{"CONFIG cluster\r\n",
			errors.New("expected 4 components in config metadata, and received 2, meta: CONFIG cluster"),
		},
		{"CONFIG cluster 0 configSize\r\n",
			errors.New("failed to parse config size from metadata: CONFIG cluster 0 configSize, error: strconv.Atoi: parsing \"configSize\": invalid syntax"),
		},
		{"CONFIG cluster 0 100\r\n",
			errors.New("failed to find config version: EOF"),
		},
		{"CONFIG cluster 0 100\r\nconfigVersion\r\n",
			errors.New("failed to parser config version: strconv.Atoi: parsing \"configVersion\": invalid syntax"),
		},
		{"CONFIG cluster 0 100\r\n0\r\n",
			errors.New("failed to read nodes: EOF"),
		},
		{"CONFIG cluster 0 0\r\n100\r\ndns-1|ip-1|11211\r\nEND\r\n",
			errors.New("expected 0 in config payload, but got 23 instead."),
		},
		{"CONFIG cluster 0 17\r\n100\r\ndns-1|ip-1\r\nEND\r\n",
			errors.New("node not in expected format: [dns-1 ip-1]"),
		},
		{"CONFIG cluster 0 22\r\n100\r\ndns-1|ip-1|port\r\nEND\r\n",
			errors.New("failed to parse port: [dns-1 ip-1 port], err: strconv.Atoi: parsing \"port\": invalid syntax"),
		},
	}

	for _, testCase := range testCases {
		reader := bufio.NewReader(strings.NewReader(testCase.content))

		_, err := resolver.parseConfig(reader)

		testutil.Assert(t, testCase.expectedErr.Error() == err.Error(), "expected error '%v', but got '%v'", testCase.expectedErr.Error(), err.Error())
	}

}
