package main

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestParseServerNameMaps(t *testing.T) {
	regexMap, err := ParseServerNameMaps([]string{
		"thanos-.*=thanos-prometheus.local",
	})
	testutil.Ok(t, err)
	for k, v := range regexMap {
		testutil.Equals(t, k.String(), "thanos-.*")
		testutil.Equals(t, v, "thanos-prometheus.local")
	}
}

func TestParseBadServerNameMaps(t *testing.T) {
	_, err := ParseServerNameMaps([]string{
		"thanos-.*=",
	})
	testutil.NotOk(t, err)
}

func TestParseBadRegexServerNameMaps(t *testing.T) {
	_, err := ParseServerNameMaps([]string{
		"[0=thanos-prometheus.local",
	})
	testutil.NotOk(t, err)
}
