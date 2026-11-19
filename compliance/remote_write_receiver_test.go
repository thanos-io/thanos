// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compliance

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/compliance/remotewrite/receiver"
)

type internalThanosReceiver struct {
	binary string
}

func (internalThanosReceiver) Name() string { return "internal-thanos-receive" }

func (r internalThanosReceiver) Run(ctx context.Context, opts receiver.Options) error {
	dir, err := os.MkdirTemp("", "test-thanos-receive-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	return receiver.RunCommand(ctx, ".", nil,
		r.binary,
		"receive",
		fmt.Sprintf("--remote-write.address=%s", opts.ReceiveHostPort),
		"--http-address=127.0.0.1:0",
		"--grpc-address=127.0.0.1:0",
		"--receive.capnproto-address=127.0.0.1:0",
		fmt.Sprintf("--tsdb.path=%s", dir),
		"--tsdb.max-exemplars=100000",
		`--label=receive_replica="0"`,
	)
}

var _ receiver.Receiver = internalThanosReceiver{}

// TestRemoteWriteReceiver runs tests from https://github.com/prometheus/compliance/tree/main/remotewrite/receiver
// against Thanos Receive.
func TestRemoteWriteReceiver(t *testing.T) {
	receiver.RunTests(
		t,
		internalThanosReceiver{binary: buildThanos(t)},
		receiver.ComplianceTests(),
		receiver.WithWritePath("/api/v1/receive"),
	)
}
