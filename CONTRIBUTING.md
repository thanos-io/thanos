# Contributing

When contributing not obvious change to Thanos repository, please first
discuss the change you wish to make via issue or slack, or any other
method with the owners of this repository before making a change.

Please follow the [code of conduct](CODE_OF_CONDUCT.md) in all your interactions with the project.

## Pull Request Process

1. Read [getting started docs](docs/getting_started.md) and prepare Thanos.
2. Familarize yourself with [Makefile](Makefile) commands like `format`, `build`, `proto` and `test`.
3. Keep PRs as small as possible. Chain them if needed (base PR on other PRs).
4. If you don't have a live object store ready add these envvars to skip tests for these:
- THANOS_SKIP_GCS_TESTS to skip GCS tests.
- THANOS_SKIP_S3_AWS_TESTS to skip AWS tests.

If you skip both of these, the store specific tests will be run against memory object storage only.
CI runs GCS and inmem tests only for now. Not having these variables will produce auth errors against GCS or AWS tests.

5. If your change affects users (adds or removes feature) consider adding the item to [CHANGELOG](CHANGELOG.md)
6. You may merge the Pull Request in once you have the sign-off of at least one developers with write access, or if you
   do not have permission to do that, you may request the second reviewer to merge it for you.
7. If you feel like your PR waits too long for a review, feel free to ping [`#thanos-dev`](https://join.slack.com/t/improbable-eng/shared_invite/enQtMzQ1ODcyMzQ5MjM4LWY5ZWZmNGM2ODc5MmViNmQ3ZTA3ZTY3NzQwOTBlMTkzZmIxZTIxODk0OWU3YjZhNWVlNDU3MDlkZGViZjhkMjc) channel on our slack for review!
