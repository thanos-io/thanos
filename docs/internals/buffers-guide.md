# Buffers guide

## Intro

This is a guide to buffers in Thanos. The goal is to show how data is moving around, what objects or copying is happening, what are the life-times of each object and so on. With this information we will be able to make better decisions on how to make the code more garbage collector (GC) friendly.

### Situation in 0.39.2

We only use protobuf encodings and compression is optional:

```
gRPC gets a compressed protobuf message -> decompress -> protobuf decoder
```

We still use gogoproto so in the protobuf decoder we specify a custom type for labels - ZLabels. This is a "hack" that uses unsafe underneath. With the `slicelabels` tag, it is possible to create labels.Labels objects (required by the PromQL layer) and reuse references to strings allocated in the protobuf layer. The protobuf message's bytes buffer is never recycled and it lives as far as possible until it is collected by the GC. Chunks and all other objects are still copied.

### gRPC gets the ability to recycle messages

Nowadays, gRPC can and does by default recycle the decoded messages nowadays so that it wouldn't be needed to allocate a new `[]byte` all the time on the gRPC layer. But this means that we have to be conscious in the allocations that we make.

Previously we had:

```go
[]struct {
	Name  string
	Value string
}
```

So, a slice of two pointers in each element plus the strings themselves. But, fortunately, we use unsafe code and we don't allocate a new string object for each string but these strings rather point to []bytes.

With `stringlabel` and careful use of `labels.ScratchBuilder` we could put all labels into one string object. Only consequence of this is that we will have to copy protobuf message's data into this special format but copying data in memory is faster (probably?) than having to iterate through possibly millions of objects during GC time.

Also, ideally we wouldn't have to allocate data for messages and stream them into the readers but if the messages are compressed then there is no way we could do that since for generic compression (in most cases) you need to have the whole message in memory. Cap N' Proto is also based on messages so you need to read a message fully. Only bonus is that it gives you full control over the lifetime of the messages. Most `grpc-go` encoders immediately put the message's buffer back after decoding it BUT it is possible to hold a reference for longer:

[CodecV2 ref](https://pkg.go.dev/google.golang.org/grpc/encoding#CodecV2)

Hence, the only possibility for further improvements at the moment it seems is to associate the life-time of messages with the query itself so that we could avoid copying `[]bytes` for the chunks (mostly).

I wrote a benchmark and it seems like `stringlabel` + hand-rolled unmarshaling code wins:

```
goos: linux
goarch: amd64
pkg: github.com/thanos-io/thanos/pkg/store/labelpb
cpu: Intel(R) Core(TM) i9-10885H CPU @ 2.40GHz
                                      │ labelsbench  │
                                      │    sec/op    │
LabelUnmarshal/Unmarshal_regular-16     123.0µ ± 41%
LabelUnmarshal/Unmarshal_ZLabel-16      65.43µ ± 40%
LabelUnmarshal/Unmarshal_easyproto-16   41.19µ ± 45%
geomean                                 69.20µ

                                      │ labelsbench  │
                                      │     B/op     │
LabelUnmarshal/Unmarshal_regular-16     84.22Ki ± 0%
LabelUnmarshal/Unmarshal_ZLabel-16      68.59Ki ± 0%
LabelUnmarshal/Unmarshal_easyproto-16   32.03Ki ± 0%
geomean                                 56.99Ki

                                      │ labelsbench │
                                      │  allocs/op  │
LabelUnmarshal/Unmarshal_regular-16     2.011k ± 0%
LabelUnmarshal/Unmarshal_ZLabel-16       11.00 ± 0%
LabelUnmarshal/Unmarshal_easyproto-16    1.000 ± 0%
geomean                                  28.07
```
