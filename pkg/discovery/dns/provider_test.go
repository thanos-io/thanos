package dns

import (
	"context"
	"testing"

	"errors"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestProvider_ShouldReturnLatestValidAddresses_WhenDiscovererReturnsErrors(t *testing.T) {
	discoverer := &mockDiscoverer{nil}
	prv := NewProvider(discoverer)
	ctx := context.TODO()

	ip1 := "127.0.0.1:19091"
	ip2 := "127.0.0.1:19092"
	ip3 := "127.0.0.1:19093"
	addrs := []string{"dns+" + ip1, "dns+" + ip2, "dns+" + ip3}
	resolved := []string{ip1, ip2, ip3}

	testutil.Ok(t, prv.Resolve(ctx, addrs))
	result := prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	for i, addr := range resolved {
		testutil.Assert(t, addr == result[i], "Expected %v but got %v", addr, result[i])
	}

	prv.resolver = &mockDiscoverer{errors.New("failed to resolve urls")}
	testutil.NotOk(t, prv.Resolve(ctx, addrs))
	result = prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	for i, addr := range resolved {
		testutil.Assert(t, addr == result[i], "Expected %v but got %v", addr, result[i])
	}
}

type mockDiscoverer struct {
	err error
}

func (d *mockDiscoverer) Resolve(ctx context.Context, name string, qtype string) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	return []string{name}, nil
}
