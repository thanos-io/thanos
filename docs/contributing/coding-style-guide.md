---
title: Coding Style Guide
type: docs
menu: contributing
---

<style>
table {
    width:100%;
}
</style>

# Thanos Coding Style Guide

This document details the official style guides for the various languages we use in the Thanos project.
Feel free to familiarize yourself with and refer to this document during code reviews. If something in our codebase does not match the style, it means it
was missed or it was written before this document. Help wanted to fix it! (:

Generally we care about:

* Readability, so low [Cognitive Load](https://www.dabapps.com/blog/cognitive-load-programming/).
* Maintainability. We avoid the code that **surprises**.
* Performance only for critical path and without compromising readability.
* Testability. Even if it means some changes to the production code, like `timeNow func() time.Time` mock.
* Consistency: If some pattern repeats, it means fewer surprises.

Some style is enforced by our linters and is covered in separate smaller sections. Please look there if you want to
embrace some of the rules in your own project! For Thanos developers we recommend to read sections about rules to manually apply during
development. Some of those are currently impossible to detect with linters. Ideally everything would be automated. (:

## TOC
- [Thanos Coding Style Guide](#thanos-coding-style-guide)
  * [TOC](#toc)
- [Go](#go)
  * [Development / Code Review](#development---code-review)
    + [Reliability](#reliability)
      - [Defers: Don't Forget to Check Returned Errors](#defers--don-t-forget-to-check-returned-errors)
      - [Exhaust Readers](#exhaust-readers)
      - [Avoid Globals](#avoid-globals)
      - [Never Use Panics](#never-use-panics)
      - [Avoid Using the `reflect` or `unsafe` Packages](#avoid-using-the--reflect--or--unsafe--packages)
      - [Avoid variable shadowing](#avoid-variable-shadowing)
    + [Performance](#performance)
      - [Pre-allocating Slices and Maps](#pre-allocating-slices-and-maps)
      - [Reuse arrays](#reuse-arrays)
    + [Readability](#readability)
      - [Keep the Interface Narrow; Avoid Shallow Functions](#keep-the-interface-narrow--avoid-shallow-functions)
      - [Use Named Return Parameters Carefully](#use-named-return-parameters-carefully)
      - [Way to Defer Something Only if Function Fails](#way-to-defer-something-only-if-function-fails)
      - [Explicitly Handled Returned Errors](#explicitly-handled-returned-errors)
      - [Only Two Ways of Formatting Functions/Methods](#only-two-ways-of-formatting-functions-methods)
      - [Control Structure: Prefer early returns and avoid `else`](#control-structure--prefer-early-returns-and-avoid--else-)
      - [Wrap Errors for More Context; Don't Repeat "failed ..." There.](#wrap-errors-for-more-context--don-t-repeat--failed---there)
      - [Use the Blank Identifier `_`](#use-the-blank-identifier----)
      - [Rules for Log Messages](#rules-for-log-messages)
      - [Comment Necessary Surprises](#comment-necessary-surprises)
    + [Testing](#testing)
      - [Table Tests](#table-tests)
      - [Tests for Packages / Structs That Involve `time` package.](#tests-for-packages---structs-that-involve--time--package)
  * [Ensured by linters](#ensured-by-linters)
      - [Avoid Prints.](#avoid-prints)
      - [Ensure Prometheus Metric Registration](#ensure-prometheus-metric-registration)
      - [go vet](#go-vet)
      - [golangci-lint](#golangci-lint)
      - [misspell](#misspell)
      - [Commentaries Should we a Full Sentence.](#commentaries-should-we-a-full-sentence)
- [Bash](#bash)

<small><i>Table of contents generated with <a href='http://ecotrust-canada.github.io/markdown-toc/'>markdown-toc</a></i></small>

# Go

For code written in [Go](https://golang.org/) we use the standard Go style guides ([Effective Go](https://golang.org/doc/effective_go.html),
[CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments)) with a few additional rules that make certain areas stricter
than the standard guides. This ensures even better consistency in modern distributed system databases like Thanos, where reliability, performance and
maintainability are extremely important.

<img src="../img/go-in-thanos.jpg" class="img-fluid" alt="Go in Thanos" />

<!--
NOTE: Because of blackfriday bug, we have to change those code snippet to `< highlight go >` hugo shortcodes during `websitepreprocessing.sh` for website.
-->

## Development / Code Review

In this section we will go through rules that on top of the standard guides that we apply during development and code reviews.

NOTE: If you know that any of those rules can be enabled by some linter, automatically, let us know! (:

### Reliability

#### Defers: Don't Forget to Check Returned Errors

It's easy to forget to check the error returned by a `Close` method that we deferred.

```go
f, err := os.Open(...)
if err != nil {
    // handle..
}
defer f.Close() // What if an error occurs here?

// Write something to file... etc.
```

Unchecked errors like this can lead to major bugs. Consider the above example: the `*os.File` `Close` method can be responsible
for actually flushing to the file, so if error occurs at that point, the whole write might be aborted! 😱

Always check errors! To make it consistent and not distracting, use our [runutil](https://pkg.go.dev/github.com/thanos-io/thanos@v0.11.0/pkg/runutil?tab=doc)
helper package, e.g.:

```go
// Use `CloseWithErrCapture` if you want to close and fail the function or
// method on a `f.Close` error (make sure thr `error` return argument is
// named as `err`). If the error is already present, `CloseWithErrCapture`
// will append (not wrap) the `f.Close` error if any.
defer runutil.CloseWithErrCapture(&err, f, "close file")

// Use `CloseWithLogOnErr` if you want to close and log error on `Warn`
// level on a `f.Close` error.
defer runutil.CloseWithLogOnErr(logger, f, "close file")
```

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
func writeToFile(...) error {
    f, err := os.Open(...)
    if err != nil {
        return err
    }
    defer f.Close() // What if an error occurs here?

    // Write something to file...
    return nil
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func writeToFile(...) (err error) {
    f, err := os.Open(...)
    if err != nil {
        return err
    }
    // Now all is handled well.
    defer runutil.CloseWithErrCapture(&err, f, "close file")

    // Write something to file...
    return nil
}
```

</td></tr>
</tbody></table>

#### Exhaust Readers

One of the most common bugs is forgetting to close or fully read the bodies of HTTP requests and responses, especially on
error. If you read body of such structures, you can use the [runutil](https://pkg.go.dev/github.com/thanos-io/thanos@v0.11.0/pkg/runutil?tab=doc)
helper as well:

```go
defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "close response")
```

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
resp, err := http.Get("http://example.com/")
if err != nil {
    // handle...
}
defer runutil.CloseWithLogOnErr(logger, resp.Body, "close response")

scanner := bufio.NewScanner(resp.Body)
// If any error happens and we return in the middle of scanning
// body, we can end up with unread buffer, which
// will use memory and hold TCP connection!
for scanner.Scan() {
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
resp, err := http.Get("http://example.com/")
if err != nil {
    // handle...
}
defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "close response")

scanner := bufio.NewScanner(resp.Body)
// If any error happens and we return in the middle of scanning body,
// defer will handle all well.
for scanner.Scan() {
```

</td></tr>
</tbody></table>

#### Avoid Globals

No globals other than `const` are allowed. Period.
This means also, no `init` functions.

#### Never Use Panics

Never use them. If some dependency use it, use [recover](https://golang.org/doc/effective_go.html#recover). Also consider
avoiding that dependency. 🙈

#### Avoid Using the `reflect` or `unsafe` Packages

Use those only for very specific, critical cases. Especially `reflect` tend to be be very slow. For testing code it's fine to use reflect.

#### Avoid variable shadowing

Variable shadowing is when you use the same variable name in a smaller scope that "shadows". This is very
dangerous as it leads to many surprises. It's extremely hard to debug such problems as they might appear in unrelated parts of the code.
And what's broken is tiny `:` or lack of it. 

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
    var client ClientInterface 
    if clientTypeASpecified {
        // Ups - typo, should be =`    
        client, err := clienttypea.NewClient(...)
        if err != nil {
            // handle err
        }    
        level.Info(logger).Log("msg", "created client", "type", client.Type)
    } else {
        // Ups - typo, should be =`  
         client, err := clienttypea.NewClient(...) 
         level.Info(logger).Log("msg", "noop client will be used", "type", client.Type)
    }   

    // In some further deeper part of the code... 
    resp, err := client.Call(....) // nil pointer panic!
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
    var client ClientInterface = NewNoop(...) 
    if clientTypeASpecified {
        c, err := clienttypea.NewClient(...)
        if err != nil {
            // handle err
        }
        client = c    
    }  
    level.Info(logger).Log("msg", "created client", "type", c.Type)
    
    resp, err := client.Call(....)
```

</td></tr>
</tbody></table>

This is also why we recommend to scope errors if you can:

```go
    if err := doSomething; err != nil {
        // handle err
    }
```

While it's not yet consider we might think of not permitting variable shadowing with [`golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow`](https://godoc.org/golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow).
There was even Go2 proposal for disabling this totally in a language, but was rejected: https://github.com/golang/go/issues/21114

Similar to this problem is the package name shadowing. While it is less dangerous, it can cause similar issue, so avoid that if you can. 

### Performance

After all, Thanos system is a database that has to perform queries over terabytes of data within human friendly response times.
This requires some additional patterns to our code. With those patterns try to not sacrifice the readability and apply those only
on critical code paths. Also always measure. The Go performance relies on many hidden things, so the good micro benchmark, following
with the real system load test is the key.

#### Pre-allocating Slices and Maps

Try to always preallocate slices and map either via `cap` or `length`. If you know the number elements you want to put
apriori, use that knowledge!  This significantly improves the latency of such code. Consider this as micro optimization,
however it's a good pattern to do it always, as it does not add much complexity. Performance wise, it's only relevant for critical,
code paths with big arrays.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
func copyIntoSliceAndMap(biggy []string) (a []string, b map[string]struct{})
    b = map[string]struct{}{}

    for _, item := range biggy {
        a = append(a, item)
        b[item] = struct{}
    }
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func copyIntoSliceAndMap(biggy []string) (a []string, b map[string]struct{})
    b = make(map[string]struct{}, len(biggy))
    a = make([]string, len(biggy))

    // Copy will not even work without pre-allocation.
    copy(a, biggy)
    for _, item := range biggy {
        b[item] = struct{}
    }
}
```

</td></tr>
</tbody></table>

#### Reuse arrays

To extend above point, there are cases where you don't need to allocate anything all the time. If you repeat certain operation on slices
sequentially, it's reasonable to reuse underlying array for those. This can give quite enormous gains for critical paths.
Unfortunately there is no way to reuse underlying array for maps.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
var messages []string{}
for _, msg := range recv {
    messages = append(messages, msg)

    if len(messages) > maxMessageLen {
        marshalAndSend(messages)
        // This creates new array. Previous array
        // will be garbage collected only after
        // some time (seconds), which
        // can create enormous memory pressure.
        messages = []string{}
    }
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
var messages []string{}
for _, msg := range recv {
    messages = append(messages, msg)

    if len(messages) > maxMessageLen {
        marshalAndSend(messages)
        // Instead of new array, reuse
        // the same, with the same capacity,
        // just length equals to zero.
        messages = messages[:0]
    }
}
```

</td></tr>
</tbody></table>

### Readability

#### Keep the Interface Narrow; Avoid Shallow Functions

This is connected more to API design than coding, but even during small coding decisions matter. For example how you define functions
or methods. There are two general rules:

* Simpler (usually it means smaller) interfaces are better. This might mean smaller function signature as well as less methods
in the interfaces. Try to group interfaces based on functionality to expose at max 1-3 methods if possible.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
type Compactor interface {
    Compact(ctx context.Context) error 
    FetchMeta(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error)
    UpdateOnMetaChange(func([]metadata.Meta, error))
    SyncMetas(ctx context.Context) error 
    Groups() (res []*Group, err error)
    GarbageCollect(ctx context.Context) error 
    ApplyRetentionPolicyByResolution(ctx context.Context, logger log.Logger, bkt objstore.Bucket) error
    BestEffortCleanAbortedPartialUploads(ctx context.Context, bkt objstore.Bucket)
    DeleteMarkedBlocks(ctx context.Context) error
    Downsample(ctx context.Context, logger log.Logger, metrics *DownsampleMetrics, bkt objstore.Bucket) error
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go

// Smaller interfaces with smaller number of arguments allow functional grouping and clean composition.

type Compactor interface {
    Compact(ctx context.Context) error 
    
}

type Downsampler interface {
    Downsample(ctx context.Context) error
}

type MetaFetcher interface {
    Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error)
    UpdateOnChange(func([]metadata.Meta, error))
}

type Syncer interface {
    SyncMetas(ctx context.Context) error 
    Groups() (res []*Group, err error)
    GarbageCollect(ctx context.Context) error 
}

type RetentionKeeper interface {
    Apply(ctx context.Context) error
}

type Cleaner interface {
    DeleteMarkedBlocks(ctx context.Context) error
    BestEffortCleanAbortedPartialUploads(ctx context.Context)
}

```

</td></tr>
</tbody></table>

* It's better if you can hide more unnecessary complexity from the user. This mean that having shallow function introduce 
more cognitive load to understand the function name or navigate to implementation to understand it better. It might be much 
more readable to inline those few lines directly on the caller side.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
    // Some code...
    s.doSomethingAndHandleError()

    // Some code...

func (s *myStruct) doSomethingAndHandleError() {
    if err := doSomething; err != nil {
        level.Error(s.logger).Log("msg" "failed to do something, sorry", "err", err)
    }
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>


```go
    // Some code...
    if err := doSomething; err != nil {
        level.Error(s.logger).Log("msg" "failed to do something, sorry", "err", err)
    }

    // Some code...
```

</td></tr>
</tbody></table>

This is a little bit connected to `There should be one-- and preferably only one --obvious way to do it` and `DRY` rules.
If you have more ways of doing something than one, you create wider interface, allowing more opportunities for errors and maintenance
burden. 

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
// We have here three ways of getting ID. Can you find all of them?

type Block struct {
    // Things...
    ID ulid.ULID
    
    mtx sync.Mutex
}

func (b *Block) Lock() {  b.mtx.Lock() }

func (b *Block) Unlock() {  b.mtx.Unlock() }

func (b *Block) ID() ulid.ULID { 
    b.mtx.Lock()
    defer b.mtx.Unlock()
    return b.ID
}

func (b *Block) IDNoLock() ulid.ULID {  return b.ID }
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
type Block struct {
    // Things...
    
    id ulid.ULID
    mtx sync.Mutex
}

func (b *Block) ID() ulid.ULID { 
    b.mtx.Lock()
    defer b.mtx.Unlock()
    return b.id
}
```

</td></tr>
</tbody></table>

#### Use Named Return Parameters Carefully

It's OK to name return parameters if the types do not give enough information about what function or method actually returns.
Another use case is when you want to define a variable, e.g. a slice.

**IMPORTANT:** never use naked `return` statements with named return parameters. This compiles but it makes returning values
implicit and thus more prone to surprises.

#### Way to Defer Something Only if Function Fails

There is a way to sacrifice defer in order to properly close all on each error. Repetitions makes it easier to make error
and forget something when changing the code, so on-error deferring is doable:

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
func OpenSomeFileAndDoSomeStuff() (*os.File, error) {
    f, err := os.OpenFile("file.txt", os.O_RDONLY, 0)
    if err != nil {
        return nil, err
    }    
    
    if err := doStuff1(); err != nil {
        runutil.CloseWithErrCapture(&err, f, "close file")
        return nil, err
    }
    if err := doStuff2(); err != nil {
        runutil.CloseWithErrCapture(&err, f, "close file")
        return nil, err
    }
    if err := doStuff232241(); err != nil {
        // Ups.. forgot to close file here.
        return nil, err
    }
    return f, nil
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func OpenSomeFileAndDoSomeStuff() (f *os.File, err error) {
    f, err = os.OpenFile("file.txt", os.O_RDONLY, 0)
    if err != nil {
        return nil, err
    }
    defer func() {
        if err != nil {
             runutil.CloseWithErrCapture(&err, f, "close file")
        }
    }   
    
    if err := doStuff1(); err != nil {
        return nil, err
    }
    if err := doStuff2(); err != nil {
        return nil, err
    }
    if err := doStuff232241(); err != nil {
        return nil, err
    }
    return f, nil
}
```
</td></tr>
</tbody></table>

#### Explicitly Handled Returned Errors

Always address returned errors. It does not mean you cannot "ignore" the error for some reason, e.g. if we know implementation
will not return anything meaningful. You can ignore the error, but do so explicitly:

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
someMethodThatReturnsError(...)
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>


```go
_ = someMethodThatReturnsError(...)
```

</td></tr>
</tbody></table>

The exception: well known cases such as `level.Debug|Warn` etc and `fmt.FPrint*`

#### Only Two Ways of Formatting Functions/Methods

Prefer function/method definitions with arguments in a single line. If it's too wide, put each argument on a new line.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
func function(argument1 int, argument2 string,
    argument3 time.Duration, argument4 someType,
    argument5 float64, argument6 time.Time,
) (ret int, err error) {
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func function(
    argument1 int,
    argument2 string,
    argument3 time.Duration,
    argument4 someType,
    argument5 float64,
    argument6 time.Time,
) (ret int, err error)
```

</td></tr>
</tbody></table>

#### Control Structure: Prefer early returns and avoid `else`

In most of the cases you don't need `else`. You can usually use `continue`, `break` or `return` to end an `if` block.
This enables having one less indent and netter consistency so code is more readable.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
for _, elem := range elems {
    if a == 1 {
        something[i] = "yes"
    } else
        something[i] = "no"
    }
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
for _, elem := range elems {
    if a == 1 {
        something[i] = "yes"
        continue
    }
    something[i] = "no"
}
```

</td></tr>
</tbody></table>

#### Wrap Errors for More Context; Don't Repeat "failed ..." There.

We use [`pkg/errors`](https://github.com/pkg/errors) package for `errors`. We prefer it over standard wrapping with `fmt.Errorf` + `%w`,
as `errors.Wrap` is explicit. It's easy to by accident replace `%w` with `%v` or to add extra inconsistent characters to the string.

Use [`pkg/errors.Wrap`](https://github.com/pkg/errors) to wrap errors for future context when errors occurs. It's recommended
to add more interesting variables to add context using `errors.Wrapf`, e.g. file names, IDs or things that fail, etc.

NOTE: never prefix wrap messages with wording like `failed ... ` or `error occurred while...`. Just describe what we
wanted to do when the failure occurred. Those prefixes are just noise. We are wrapping error, so it's obvious that some error
occurred, right? (: Improve readability and consider avoiding those.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
if err != nil {
    return fmt.Errorf("error while reading from file %s: %w", f.Name, err)
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
if err != nil {
    return errors.Wrapf(err, "read file %s", f.Name)
}
```

</td></tr>
</tbody></table>

#### Use the Blank Identifier `_`

Black identifiers are very useful to mark variables that are not used. Consider the following cases:

```go
// We don't need the second return parameter.
// Let's use the blank identifier instead.
a, _, err := function1(...)```

```go
// We don't need to use this variable, we
// just want to make sure TypeA implements InterfaceA.
var _ InterfaceA = TypeA
```

```go
// We don't use context argument; let's use the blank
// identifier to make it clear.
func (t *Type) SomeMethod(_ context.Context, abc int) error {
```

#### Rules for Log Messages

We use [go-kit logger](https://github.com/go-kit/kit/tree/master/log) in Thanos. This means that we expect log lines
to have certain structure. Structure means that instead of adding variables to the message, those should be passed as the 
separate fields. Keep in mind that all in Thanos should be `lowercase` (readability and consistency) and
all struct keys are using `camelCase`. explain all. It's suggested to keep key names short and consistent. (For example if
we always use `block` for block ID, let's not use in the other single log message `id`)

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
level.Info(logger).Log("msg", fmt.Sprintf("Found something epic during compaction number %v. This looks amazing.", compactionNumber), 
 "block_id", id, "elapsed-time", timeElapsed)
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
level.Info(logger).Log("msg", "found something epic during compaction; this looks amazing", "compNumber", compNumber,  
"block", id, "elapsed", timeElapsed)
```

</td></tr>
</tbody></table>

Additionally there are certain rules we suggest while using different log levels:

* level.Info: Should always have `msg` field. Should be used only for important events that we expect to happen not too
often.
* level.Debug: Should always have `msg` field. Can be bit more spammy, but should not be everywhere as well. Use it
only when you want to really dive into some problem in certain area.
* level.Warn: Should have either `msg` or `err` or both fields. They should warn about events that are suspicious and to investigate
but process can gracefully mitigate it. Always try to describe *how* it was mitigated, what action will be performed e.g. `value will be skipped`
* level.Error: Should have either `msg` or `err` or both fields. Use it only for critical event.

#### Comment Necessary Surprises

Comments are not the best. They age quickly and compiler does not fail if you will forget to update them. So use comments
only when necessary. **And it is necessary to comment on code that can surprise user.** Sometimes, complexity
is necessary, for example for performance. Comment in this case why such optimization was needed. If something
was done temporarily add `TODO(<github name>): <something, with GitHub issue link ideally>` to not forget about
that. 

### Testing

#### Table Tests

Use table-driven tests that use [t.Run](https://blog.golang.org/subtests) for readability. They are easy to read
and allows to add clean description of each test case. Adding or adapting test cases is also easier.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>

```go
host, port, err := net.SplitHostPort("1.2.3.4:1234")
testutil.Ok(t, err)
testutil.Equals(t, "1.2.3.4", host)
testutil.Equals(t, "1234", port)

host, port, err = net.SplitHostPort("1.2.3.4:something")
testutil.Ok(t, err)
testutil.Equals(t, "1.2.3.4", host)
testutil.Equals(t, "http", port)

host, port, err = net.SplitHostPort(":1234")
testutil.Ok(t, err)
testutil.Equals(t, "", host)
testutil.Equals(t, "1234", port)

host, port, err = net.SplitHostPort("yolo")
testutil.NotOk(t, err)
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>

```go
for _, tcase := range []struct{
    name string

    input     string

    expectedHost string
    expectedPort string
    expectedErr error
}{
    {
        name: "host and port",

        input:     "1.2.3.4:1234",
        expectedHost: "1.2.3.4",
        expectedPort: "1234",
    },
    {
        name: "host and named port",

        input:     "1.2.3.4:something",
        expectedHost: "1.2.3.4",
        expectedPort: "something",
    },
    {
        name: "just port",

        input:     ":1234",
        expectedHost: "",
        expectedPort: "1234",
    },
    {
        name: "not valid hostport",

        input:     "yolo",
        expectedErr: errors.New("<exact error>")
    },
}{
    t.Run(tcase.name, func(t *testing.T) {
        host, port, err := net.SplitHostPort(tcase.input)
        if tcase.expectedErr != nil {
            testutil.NotOk(t, err)
            testutil.Equals(t, tcase.expectedErr, err)
            return
        }
        testutil.Ok(t, err)
        testutil.Equals(t, tcase.expectedHost, host)
        testutil.Equals(t, tcase.expectedPort, port)
    })
}
```

</td></tr>
</tbody></table>

#### Tests for Packages / Structs That Involve `time` package.

Avoid unit testing based on real time. Always try to mock time that is used within struct by using for example `timeNow func() time.Time` field.
For production code, you can initialize the field with `time.Now`. For test code, you can set a custom time that will be used by the struct.

<table>
<thead align="center"><tr><th>Avoid 🔥</th></tr></thead>
<tbody>
<tr><td>


```go
func (s *SomeType) IsExpired(created time.Time) bool {
    // Code is hardly testable.
    return time.Since(created) >= s.expiryDuration
}
```

</td></tr>
</tbody></table>
<table>
<thead align="center"><tr><th>Better 🤓</th></tr></thead>
<tbody>
<tr><td>


```go
func (s *SomeType) IsExpired(created time.Time) bool {
    // s.timeNow is time.Now on production, mocked in tests.
    return created.Add(s.expiryDuration).After(s.timeNow())
}
```

</td></tr>
</tbody></table>

## Ensured by linters

This is the list of rules we ensure automatically. This section is for those who are curious why such linting rules
were added or want to maybe add similar to their project.

#### Avoid Prints.

Never use `print`. Always use a passed `go-kit/log.Logger`.

#### Ensure Prometheus Metric Registration

#### go vet

#### golangci-lint

#### misspell

#### Commentaries Should we a Full Sentence.

# Bash

Overall try to NOT use bash. For scripts longer than 30 lines, consider writing it in Go as we did [here](https://github.com/thanos-io/thanos/blob/55cb8ca38b3539381dc6a781e637df15c694e50a/scripts/copyright/copyright.go).

If you have to, we follow the Google Shell style guide: https://google.github.io/styleguide/shellguide.html
