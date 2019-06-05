package receive

import (
	"errors"
	"sort"

	"github.com/improbable-eng/thanos/pkg/store/prompb"

	"github.com/cespare/xxhash"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const sep = '\xff'

// Hashring finds the correct host to handle a given time series
// for a specified tenant.
// It returns the hostname and any error encountered.
type Hashring interface {
	GetHost(tenant string, timeSeries *prompb.TimeSeries) (string, error)
}

// Matcher determines whether or tenant matches a hashring.
type Matcher interface {
	Match(tenant string, hashring string) bool
}

// MultiMatcher is a list of Matchers that implements the Matcher interface.
type MultiMatcher []Matcher

// Match implements the Matcher interface.
func (m MultiMatcher) Match(tenant, hashring string) bool {
	for i := range m {
		if !m[i].Match(tenant, hashring) {
			return false
		}
	}
	return true
}

// MatcherFunc is a shim to use a func as a Matcher.
type MatcherFunc func(string, string) bool

// Match implements the Matcher interface.
func (m MatcherFunc) Match(tenant, hashring string) bool {
	return m(tenant, hashring)
}

// ExactMatcher is a matcher that checks if the tenant exactly matches the hashring name.
var ExactMatcher = MatcherFunc(func(tenant, hashring string) bool { return tenant == hashring })

// hash returns a hash for the given tenant and time series.
func hash(tenant string, ts *prompb.TimeSeries) uint64 {
	// Sort labelset to ensure a stable hash.
	sort.Slice(ts.Labels, func(i, j int) bool { return ts.Labels[i].Name < ts.Labels[j].Name })

	b := make([]byte, 0, 1024)
	b = append(b, []byte(tenant)...)
	b = append(b, sep)
	for _, v := range ts.Labels {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

// simpleHashring represents a group of hosts handling write requests.
type simpleHashring struct {
	targetgroup.Group
}

// GetHost returns a hostname to handle the given tenant and time series.
func (s *simpleHashring) GetHost(tenant string, ts *prompb.TimeSeries) (string, error) {
	// Always return nil here to implement the Hashring interface.
	return string(s.Targets[hash(tenant, ts)%uint64(len(s.Targets))][model.AddressLabel]), nil
}

// matchingHashring represents a set of hashrings.
// Which hashring to use is determined by the matcher.
type matchingHashring struct {
	cache     map[string]Hashring
	hashrings map[string]Hashring
	matcher   Matcher
}

// GetHost returns a hostname to handle the given tenant and time series.
func (m matchingHashring) GetHost(tenant string, ts *prompb.TimeSeries) (string, error) {
	if h, ok := m.cache[tenant]; ok {
		return h.GetHost(tenant, ts)
	}
	for name := range m.hashrings {
		if m.matcher.Match(tenant, name) {
			m.cache[tenant] = m.hashrings[name]
			return m.hashrings[name].GetHost(tenant, ts)
		}
	}
	return "", errors.New("no matching hosts to handle tenant")
}

// NewHashring creates a multi-tenant hashring for a given slice of
// groups. Which tenant's hashring to use is determined by the Matcher.
func NewHashring(matcher Matcher, groups []*targetgroup.Group) Hashring {
	m := matchingHashring{
		cache:     make(map[string]Hashring),
		hashrings: make(map[string]Hashring),
		matcher:   matcher,
	}
	for _, g := range groups {
		m.hashrings[g.Source] = &simpleHashring{*g}
	}
	return m
}
