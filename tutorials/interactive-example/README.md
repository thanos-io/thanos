# Interactive Example

Thanos repository contains an interactive example, which will spin up an environment for you locally in Docker containers.

A prerequisite for running the example is having [Docker](https://docs.docker.com/get-docker/) installed already.

To run the example, run the following commands:
1. Build the Thanos image locally by running `make docker`.
2. Navigate to the `examples/interactive` directory.
3. Comment [this line](https://github.com/thanos-io/thanos/blob/bd134d7a823708fa135e7a6931e76f581be5f879/examples/interactive/interactive_test.go#L92) in the file `interactive_test.go`.
4. `go test interactive_test.go -test.timeout=9999m`.

The example will generate some data for you to play with and store it in `data` directory for subsequent tests runs.
You can choose from different hardcoded generation profiles, which will give you different amount of data. You can change this by setting the `BLOCK_PROFILE` environment variable to a selected profile name. You can find the available profiles [here](https://github.com/thanos-io/thanosbench/blob/master/pkg/blockgen/profiles.go#L28) (we use `thanosbench` tool to generate our test data).

The default profile is `continuous-30d-tiny` which will give you test data for 5 different applications with single metric. If you really want to ramp it up, you can use `continuous-1w-small`, which will give you test data for 100 applications with 100 metrics for each (BEWARE: Generating this much data requires a lot of RAM, make sure you have at least 8 GB available, otherwise generation might fail).
