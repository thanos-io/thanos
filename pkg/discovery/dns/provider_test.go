package dns

import (
	"context"
	"testing"

	"errors"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestProvider_ShouldReturnLatestValidAddresses_WhenDiscovererReturnsErrors(t *testing.T) {
	discoverer := &mockDiscoverer{nil}
	prv := NewProvider(discoverer, log.NewNopLogger())
	ctx := context.TODO()

	ip1 := "127.0.0.1:19091"
	ip2 := "127.0.0.1:19092"
	ip3 := "127.0.0.1:19093"
	addrs := []string{"dns+" + ip1, "dns+" + ip2, "dns+" + ip3}

	testutil.Ok(t, prv.Resolve(ctx, addrs))
	result := prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, contains(result, ip1), "Expected %v but it was missing", ip1)
	testutil.Assert(t, contains(result, ip2), "Expected %v but it was missing", ip2)
	testutil.Assert(t, contains(result, ip3), "Expected %v but it was missing", ip3)

	prv.resolver = &mockDiscoverer{errors.New("failed to resolve urls")}
	testutil.Ok(t, prv.Resolve(ctx, addrs))
	result = prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, contains(result, ip1), "Expected %v but it was missing", ip1)
	testutil.Assert(t, contains(result, ip2), "Expected %v but it was missing", ip2)
	testutil.Assert(t, contains(result, ip3), "Expected %v but it was missing", ip3)
}

func TestProvider_ShouldNotReturnOldAddresses_WhenNotRequestedAnymore(t *testing.T) {
	discoverer := &mockDiscoverer{nil}
	prv := NewProvider(discoverer, log.NewNopLogger())
	ctx := context.TODO()

	ip1 := "127.0.0.1:19091"
	ip2 := "127.0.0.1:19092"
	ip3 := "127.0.0.1:19093"

	testutil.Ok(t, prv.Resolve(ctx, []string{"dns+" + ip1, "dns+" + ip2, "dns+" + ip3}))
	result := prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, contains(result, ip1), "Expected %v but it was missing", ip1)
	testutil.Assert(t, contains(result, ip2), "Expected %v but it was missing", ip2)
	testutil.Assert(t, contains(result, ip3), "Expected %v but it was missing", ip3)

	testutil.Ok(t, prv.Resolve(ctx, []string{"dns+" + ip1, "dns+" + ip2}))
	result = prv.Addresses()
	testutil.Assert(t, len(result) == 2, "Expected 2 addresses but got %v", len(result))
	testutil.Assert(t, contains(result, ip1), "Expected %v but it was missing", ip1)
	testutil.Assert(t, contains(result, ip2), "Expected %v but it was missing", ip2)
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
