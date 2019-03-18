package dns

import (
	"context"
	"sort"
	"testing"

	"github.com/pkg/errors"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestProvider(t *testing.T) {
	ips := []string{
		"127.0.0.1:19091",
		"127.0.0.2:19092",
		"127.0.0.3:19093",
		"127.0.0.4:19094",
		"127.0.0.5:19095",
	}

	prv := NewProvider(log.NewNopLogger(), nil)
	prv.resolver = &mockResolver{
		res: map[string][]string{
			"a": ips[:2],
			"b": ips[2:4],
			"c": {ips[4]},
		},
	}
	ctx := context.TODO()

	prv.Resolve(ctx, []string{"any+x"})
	result := prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, []string(nil), result)

	prv.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	result = prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, ips, result)

	prv.Resolve(ctx, []string{"any+b", "any+c"})
	result = prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, ips[2:], result)

	prv.Resolve(ctx, []string{"any+x"})
	result = prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, []string(nil), result)

	prv.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	result = prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, ips, result)

	prv.resolver = &mockResolver{err: errors.New("failed to resolve urls")}
	prv.Resolve(ctx, []string{"any+a", "any+b", "any+c"})
	result = prv.Addresses()
	sort.Strings(result)
	testutil.Equals(t, ips, result)
}

type mockResolver struct {
	res map[string][]string
	err error
}

func (d *mockResolver) Resolve(ctx context.Context, name string, qtype QType) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.res[name], nil
}
