// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	mdns "github.com/miekg/dns"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos/pkg/component"
)

func TestPerEndpointTLSConfig(t *testing.T) {
	globalTLSConfig := &tlsConfig{
		Enabled:  new(true),
		CertFile: new("/mock-path.pem"),
		KeyFile:  new("/mock-key-path.pem"),
		CAFile:   new("/mock-ca-path.pem"),
	}

	var (
		useGlobalConfigCount = 0
		tlsEnabledNilCount   = 0
		tlsEnabledTrueCount  = 0
		tlsEnabledFalseCount = 0
	)

	var testCases = []struct {
		endpointConfig        endpointSettings
		testName              string
		endpointEnableChanged bool
	}{
		{
			testName: "Empty endpoint config uses global TLS settings",
			endpointConfig: endpointSettings{
				Address:      "store1:9091",
				ClientConfig: clientConfig{},
			},
		},
		{
			testName: "Enabled is Nil but CertFile provided so will",
			endpointConfig: endpointSettings{
				Address: "store2:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{CertFile: new("/mock-endpoint-cert-path.pem")},
				},
			},
			endpointEnableChanged: true,
		},
		{
			testName: "Only MinVersion set, inherits global enabled",
			endpointConfig: endpointSettings{
				Address: "store3:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{MinVersion: new("1.2")},
				},
			},
			endpointEnableChanged: true,
		},
		{
			testName: "Enabled is explicitly set to true use defaults",
			endpointConfig: endpointSettings{
				Address: "store4:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{Enabled: new(true)},
				},
			},
		},
		{
			testName: "Enabled explicitly set to false",
			endpointConfig: endpointSettings{
				Address: "store5:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{Enabled: new(false)},
				},
			},
		},
	}

	for _, tc := range testCases {
		ecfg := tc.endpointConfig
		tlsEnabled := ecfg.ClientConfig.TLSConfig.Enabled
		useGlobalConfig := ecfg.ClientConfig.UseGlobalTLSOpts()

		if useGlobalConfig {
			useGlobalConfigCount++
		} else if tlsEnabled == nil {
			ecfg.ClientConfig.TLSConfig.applyDefaults(globalTLSConfig)
			tlsEnabledNilCount++

			// only checking if we inherited globalTLSConfig.Enabled
			// we are testing only global as enabled so just checking enabled=true
			if tc.endpointEnableChanged {
				testutil.Assert(t, ecfg.ClientConfig.TLSConfig.Enabled != nil, "%s: enabled should not be nil after merge", tc.testName)
				testutil.Equals(t, true, *ecfg.ClientConfig.TLSConfig.Enabled, "%s: enabled should be inferred as true", tc.testName)
			}
		} else if *tlsEnabled {
			ecfg.ClientConfig.TLSConfig.applyDefaults(nil)
			tlsEnabledTrueCount++
		} else {
			tlsEnabledFalseCount++
		}
	}

	testutil.Equals(t, 1, useGlobalConfigCount)
	testutil.Equals(t, 2, tlsEnabledNilCount)
	testutil.Equals(t, 1, tlsEnabledTrueCount)
	testutil.Equals(t, 1, tlsEnabledFalseCount)
}

func TestSetupEndpointSetInitialDNS(t *testing.T) {
	// net creates the channel guarding its resolver configuration on first use. Create it
	// outside the bubble, otherwise lookups inside one fail with "send on synctest channel
	// from outside bubble".
	_, _ = pipeDNSResolver(time.Now(), time.Hour, mdns.RcodeSuccess).LookupIPAddr(context.Background(), "init.test.")

	const dnsSDInterval = 30 * time.Second
	for _, tc := range []struct {
		name              string
		addresses         []string
		recoverAfter      time.Duration
		rcode             int
		wait              bool
		blockLookup       bool
		cancelAfter       time.Duration
		wantFirstUpdateAt time.Duration
	}{
		{name: "no endpoints", wait: true},
		{name: "empty DNS response", addresses: []string{"dns+endpoint.test.:10901"}, wait: true},
		{name: "NXDOMAIN", addresses: []string{"dns+endpoint.test.:10901"}, rcode: mdns.RcodeNameError, wait: true},
		{name: "transient DNS failure", addresses: []string{"dns+endpoint.test.:10901"}, recoverAfter: 10 * time.Second, wait: true, wantFirstUpdateAt: 10 * time.Second},
		{name: "DNS failure longer than the interval", addresses: []string{"dns+endpoint.test.:10901"}, recoverAfter: 45 * time.Second, wait: true, wantFirstUpdateAt: 46 * time.Second},
		{name: "DNS failure longer than the interval, without waiting", addresses: []string{"dns+endpoint.test.:10901"}, recoverAfter: 45 * time.Second, wantFirstUpdateAt: dnsSDInterval},
		{name: "cancellation during persistent DNS failure", addresses: []string{"dns+endpoint.test.:10901"}, recoverAfter: time.Hour, wait: true, cancelAfter: 35 * time.Second},
		{name: "cancellation during DNS lookup", addresses: []string{"dns+endpoint.test.:10901"}, wait: true, blockLookup: true, cancelAfter: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalResolver := net.DefaultResolver
			// Restore only after synctest.Test has waited for all lookup goroutines to exit.
			t.Cleanup(func() { net.DefaultResolver = originalResolver })
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				net.DefaultResolver = pipeDNSResolver(start, tc.recoverAfter, tc.rcode)
				if tc.blockLookup {
					net.DefaultResolver.Dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}
				}

				var g run.Group
				endpointSet, err := setupEndpointSet(
					&g,
					component.Query,
					prometheus.NewRegistry(),
					log.NewNopLogger(),
					nil,
					time.Minute,
					nil,
					time.Minute,
					tc.addresses,
					nil,
					nil,
					nil,
					"golang",
					dnsSDInterval,
					tc.wait,
					time.Minute,
					time.Second,
					time.Second,
					nil,
					&tlsConfig{},
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					"none",
					nil,
				)
				testutil.Ok(t, err)

				var stopOnce sync.Once
				stop := make(chan struct{})
				g.Add(func() error {
					<-stop
					return nil
				}, func(error) {
					stopOnce.Do(func() { close(stop) })
				})
				done := make(chan error, 1)
				go func() { done <- g.Run() }()
				defer func() {
					stopOnce.Do(func() { close(stop) })
					testutil.Ok(t, <-done)
					endpointSet.Close()
					if tc.cancelAfter > 0 {
						testutil.Equals(t, tc.cancelAfter, time.Since(start))
					}
				}()

				if tc.cancelAfter > 0 {
					ctx, cancel := context.WithTimeout(context.Background(), tc.cancelAfter)
					defer cancel()
					testutil.Equals(t, context.DeadlineExceeded, endpointSet.WaitForFirstUpdate(ctx))
				} else {
					testutil.Ok(t, endpointSet.WaitForFirstUpdate(context.Background()))
					testutil.Equals(t, tc.wantFirstUpdateAt, time.Since(start))
				}
			})
		})
	}
}

// pipeDNSResolver answers every query over net.Pipe with rcode and no records, and fails
// with a timeout until recoverAfter has passed since start.
func pipeDNSResolver(start time.Time, recoverAfter time.Duration, rcode int) *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		if time.Since(start) < recoverAfter {
			return nil, &net.DNSError{Err: "simulated DNS timeout", IsTimeout: true}
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			size := make([]byte, 2)
			if _, err := io.ReadFull(server, size); err != nil {
				return
			}
			packet := make([]byte, binary.BigEndian.Uint16(size))
			if _, err := io.ReadFull(server, packet); err != nil {
				return
			}
			var request, response mdns.Msg
			if err := request.Unpack(packet); err != nil {
				return
			}
			response.SetRcode(&request, rcode)
			response.Authoritative = true
			answer, err := response.Pack()
			if err != nil {
				return
			}
			binary.BigEndian.PutUint16(size, uint16(len(answer)))
			_, _ = server.Write(append(size, answer...))
		}()
		return client, nil
	}}
}
