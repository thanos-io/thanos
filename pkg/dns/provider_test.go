package dns

import (
	"context"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"net/url"
	"errors"
)

func TestProvider_ShouldReturnLatestValidAddresses_WhenDiscovererReturnsErrors(t *testing.T) {
	discoverer := &mockDiscoverer{nil}
	prv := NewProvider(discoverer)
	ctx := context.TODO()

	addrs := []string{"127.0.0.1:19091", "127.0.0.1:19092", "127.0.0.1:19093"}

	testutil.Ok(t, prv.Resolve(ctx, addrs, 0))
	result := prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, addrs[0] == result[0], "Expected %v but got %v", addrs[0], result[0])
	testutil.Assert(t, addrs[1] == result[1], "Expected %v but got %v", addrs[1], result[1])
	testutil.Assert(t, addrs[2] == result[2], "Expected %v but got %v", addrs[2], result[2])

	prv.discoverer = &mockDiscoverer{errors.New("failed to resolve urls")}
	testutil.NotOk(t, prv.Resolve(ctx, []string{"127.0.0.1:1111"}, 0))
	result = prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, addrs[0] == result[0], "Expected %v but got %v", addrs[0], result[0])
	testutil.Assert(t, addrs[1] == result[1], "Expected %v but got %v", addrs[1], result[1])
	testutil.Assert(t, addrs[2] == result[2], "Expected %v but got %v", addrs[2], result[2])
}

type mockDiscoverer struct {
	err error
}

func (d *mockDiscoverer) Resolve(ctx context.Context, addrs []string, defaultPort int) ([]*url.URL, error) {
	if d.err != nil {
		return nil, d.err
	}

	var urls []*url.URL
	for _, addr := range addrs {
		urls = append(urls, &url.URL{Host:addr})
	}
	return urls, nil
}