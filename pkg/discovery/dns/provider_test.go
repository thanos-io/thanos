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

	prv := NewProvider(log.NewNopLogger(), nil, "")

	m := &mockResolver{
		res: map[string]map[QType][]string{
			"a": {
				A: ips[:2],
			},
			"b": {
				SRV: ips[2:4],
			},
			"c": {
				SRVNoA: {ips[4]},
			},
		},
	}
	prv.resolver = m

	for _, tcase := range []struct {
		in       []string
		mockErr  error
		expected []string
	}{
		{in: []string{"dns+x"}, expected: []string(nil)},
		{in: []string{"dnssrv+b", "dnssrvnoa+c"}, expected: ips[2:]},
		{in: []string{"dns+x"}, expected: []string(nil)},
		{in: []string{"dns+a", "dnssrv+b", "dnssrvnoa+c"}, expected: ips},
		{in: []string{"dns+x"}, expected: []string(nil)},
		{in: []string{"dns+a", "dnssrv+b", "dnssrvnoa+c"}, expected: ips},
		// Old record should be remembered.
		{
			in:       []string{"dns+a", "dnssrv+b", "dnssrvnoa+c"},
			mockErr:  errors.New("failed to resolve urls"),
			expected: ips,
		},
		{in: []string{"http://a"}, expected: []string{"http://a"}},
		{in: []string{"https://a"}, expected: []string{"https://a"}},
		// TODO(bwplotka):https://github.com/improbable-eng/thanos/issues/1025
		// This is wrong as we expected to lookup "a".
		{in: []string{"dns+http://a"}, expected: nil},
	} {
		if ok := t.Run("", func(t *testing.T) {
			m.err = tcase.mockErr

			prv.Resolve(context.TODO(), tcase.in)
			result := prv.Addresses()
			sort.Strings(result)
			testutil.Equals(t, tcase.expected, result)
		}); !ok {
			return
		}
	}
}

type mockResolver struct {
	res map[string]map[QType][]string
	err error
}

func (d *mockResolver) Resolve(ctx context.Context, name string, qtype QType) ([]string, error) {
	if d.err != nil {
		return nil, d.err
	}

	r, ok := d.res[name]
	if !ok {
		return nil, nil
	}
	return r[qtype], nil
}
