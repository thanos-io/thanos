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

	addrs := []string{"127.0.0.1:19091", "127.0.0.1:19092", "127.0.0.1:19093"}

	testutil.Ok(t, prv.Resolve(ctx, addrs, 0))
	result := prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, addrs[0] == result[0], "Expected %v but got %v", addrs[0], result[0])
	testutil.Assert(t, addrs[1] == result[1], "Expected %v but got %v", addrs[1], result[1])
	testutil.Assert(t, addrs[2] == result[2], "Expected %v but got %v", addrs[2], result[2])

	prv.resolver = &mockDiscoverer{errors.New("failed to resolve urls")}
	testutil.NotOk(t, prv.Resolve(ctx, []string{"dns+mydomain.com"}, 0))
	result = prv.Addresses()
	testutil.Assert(t, len(result) == 3, "Expected 3 addresses but got %v", len(result))
	testutil.Assert(t, addrs[0] == result[0], "Expected %v but got %v", addrs[0], result[0])
	testutil.Assert(t, addrs[1] == result[1], "Expected %v but got %v", addrs[1], result[1])
	testutil.Assert(t, addrs[2] == result[2], "Expected %v but got %v", addrs[2], result[2])
}

type mockDiscoverer struct {
	err error
}

func (d *mockDiscoverer) Resolve(ctx context.Context, name string, qtype string, defaultPort int) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	return []string{name}, nil
}
