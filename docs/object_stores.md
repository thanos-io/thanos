# Object Storages

Thanos supports any object stores that can be implemented against Thanos [objstore.Bucket inteface](/pkg/objstore/objstore.go)

Current object storage client implementations:

| Provider             | Maturity | Auto-tested on CI | Maintainers |
|----------------------|-------------------|-----------|---------------|
| Google Cloud Storage | Stable  (production usage)             | yes       | @bplotka   |
| AWS S3               | Beta  (working PoCs, testing usage)               | no        | ?          |

NOTE: Currently Thanos requires strong consistency (write-read) for object store implementation.

## How to add a new client?

1. Create new directory under `pkg/objstore/<provider>`
2. Implement [objstore.Bucket inteface](/pkg/objstore/objstore.go)
3. Add `NewTestBucket` constructor for testing purposes, that creates and deletes temporary bucket.
4. Use created `NewTestBucket` in [ForeachStore method](/pkg/objstore/objtesting/foreach.go) to ensure we can run tests (optionally) against new provider.
5. RUN the [TestObjStoreAcceptanceTest](/pkg/objstore/objtesting/acceptance_test.go) against your provider to ensure it fits. Fix any found error until test passes.
6. Implement proper way of constructing client in [factory](/pkg/objstore/client/factory.go) code. (Using as small amount of flags as possible in every command)

At that point, anyone can use your provider!