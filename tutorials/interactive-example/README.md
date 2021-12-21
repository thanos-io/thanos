# Interactive Example

The interactive example in `examples/interactive` can be run by Linux users. It is not yet supported for MacOS users. Refer to [#4642](https://github.com/thanos-io/thanos/issues/4642) to track the status of this issue.

To run the example, run the following commands:
1. Navigate to the `examples/interactive` directory.
2. Comment [this line](https://github.com/thanos-io/thanos/blob/bd134d7a823708fa135e7a6931e76f581be5f879/examples/interactive/interactive_test.go#L92) in the file `interactive_test.go`.
3. `go test interactive_test.go -test.timeout=9999m`.
