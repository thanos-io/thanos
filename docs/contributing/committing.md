# Tips for committing

## Prefer & run short tests

Ideally, each test should be parallel i.e. not use any any shared state and run as fast as possible.

Consider using `synctest` to make the tests not timing dependent. If it is still not possible then please add this clause to your tests to skip them if `-short` is passed:

```go
if testing.Short() {
	t.Skip()
}
```

We have a suite of tests that can run very quickly. It's recommended to run them on every commit. Do that with:

```
make test-local-short
```

We have an intention to provide pre-commit hooks that would automate this workflow in the future.
