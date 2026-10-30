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

// TODO(bwplotka): Unskip tests once RW 2.0-rc.6 receiver compliance fixes are merged.
// NOTE: Keep patterns ordered by slash count ascending so longer wildcard patterns do not
// return a partial match in testing.alternationMatch before shorter exact patterns are checked.
var receiverSkips = []string{
	// 4 segments.
	`^TestRemoteWriteReceiver/internal-thanos-receive/samples/out_of_order_rejected$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/symbols/.*`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/histograms/bad_count_histogram$`,
	// 5 segments.
	`^TestRemoteWriteReceiver/internal-thanos-receive/histograms/float/bad_count_float_histogram$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/histograms/invalid_rejected/.*`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/exemplars/validation/(exemplar_without_name_label|exemplar_with_empty_name|odd_exemplar_labels_refs|out_of_bounds_exemplar_labels_refs)$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/metadata/validation/(metadata_without_name_label|metadata_with_empty_name|out_of_bounds_help_ref|out_of_bounds_unit_ref)$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/combined/edge_cases/metadata_without_data$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/request/(bad_content_types|bad_content_type_modes|unsupported_content_type_accept_header|bad_content_encodings|unsupported_content_encoding_accept_encoding_header)/.*`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/.*/.*/returns_204$`,
	// 6 segments.
	`^TestRemoteWriteReceiver/internal-thanos-receive/samples/single/.*/(simple_metric_without_name_label|empty_metric)$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/samples/multiple/.*/multiple_metrics(_1)?_without_name_label$`,
	`^TestRemoteWriteReceiver/internal-thanos-receive/.*/.*/.*/returns_204$`,
	// 7 segments.
	`^TestRemoteWriteReceiver/internal-thanos-receive/.*/.*/.*/.*/returns_204$`,
}

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
