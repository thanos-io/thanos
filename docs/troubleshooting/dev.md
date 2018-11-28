# Troubleshooting for dev workflow

## Dep `grouped write of manifest, lock and vendor: scratch directory ... already exists, please remove it`

Outcome: `make deps` fails with output: `grouped write of manifest, lock and vendor: scratch directory $GOPATH/src/github.com/improbable-eng/thanos/.vendor-new already exists, please remove it`

Reason: dep interrupted in the middle of processing.

Fix: `rm -rf $GOPATH/src/github.com/improbable-eng/thanos/.vendor-new`

## Dep `failed to unpack tree object`

Outcome: `make deps` fails with output: `grouped write of manifest, lock and vendor: failed to export github.com/Azure/azure-storage-blob-go: fatal: failed to unpack tree object 5152f14ace1c6db66bd9cb57840703a8358fa7bc`

Reason: unknown

Fix: `rm -rf $GOPATH/pkg/dep/sources/https---github.com-Azure-azure--storage--blob--go `
