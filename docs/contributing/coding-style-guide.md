---
title: Coding Style Guide
type: docs
menu: contributing
---

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

### TOC

- [Thanos Coding Style Guide](#thanos-coding-style-guide)
    + [TOC](#toc)
  * [Go](#go)
    + [Development / Code Review](#development---code-review)
      - [Only two ways of formatting functions/methods](#only-two-ways-of-formatting-functions-methods)
      - [Control Structure: Avoid else](#control-structure--avoid-else)
      - [Avoid globals](#avoid-globals)
      - [Prints.](#prints)
      - [Use name result/return parameters carefully](#use-name-result-return-parameters-carefully)
      - [Explicitly handled return errors](#explicitly-handled-return-errors)
      - [Defers: Dont' forget to check returned errors](#defers--dont--forget-to-check-returned-errors)
      - [Exhaust readers](#exhaust-readers)
      - [Wrap errors for more context. Don't repeat "failed ..." there.](#wrap-errors-for-more-context-don-t-repeat--failed---there)
      - [Never use panics](#never-use-panics)
      - [Pre-allocating slices and maps](#pre-allocating-slices-and-maps)
      - [Don't use reflect or unsafe packages](#don-t-use-reflect-or-unsafe-packages)
      - [Use blank identifier `_`](#use-blank-identifier----)
      - [Tests for packages / structs that involve time package.](#tests-for-packages---structs-that-involve-time-package)
    + [Ensured by linters](#ensured-by-linters)
  * [Bash](#bash)
  * [Javascript](#javascript)

<small><i>Table of contents generated with <a href='http://ecotrust-canada.github.io/markdown-toc/'>markdown-toc</a></i></small>

## Go

<img src="../img/go-in-thanos.jpg" class="img-fluid" alt="Go in Thanos" />

For code written in [Go](https://golang.org/) we use the standard Go style guides ([Effective Go](https://golang.org/doc/effective_go.html),
[CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments)) with a few additional rules that make certain areas stricter
than the standard guides. This ensures even better consistency in modern distributed system databases like Thanos, where reliability, performance and 
maintainability are extremely important. See more rationales behind some of the rules in [bwplotka's blog post](todo).

<!-- 
Things skipped, since covered in Effective Go or CodeReviewsComments already:
* 

TODO still: 
* log lowercase
* errors.Cause
* variable shadowing, package name shadowing
* ever do this on an exported type. If you embed sync.Mutex that makes "Lock" and "Unlock" part of your exported interface. Now the caller of your API doesn't know if they are supposed to call Lock/Unlock, or if this is a pure internal implementation detail.
* Comment on surprises.
* Avoid configs vs arguments
* Unsafe convert

NOTE: Because of blackfriday bug, we have to change those code ` snippet to < highlight go > hugo shortcodes during `websitepreprocessing.sh` for website. 
-->

### Development / Code Review 

In this section we will go through rules that on top of the standard guides that we apply during development and code reviews.

NOTE: If you know that any of those rules can be enabled by some linter, automatically, let us know! (:

#### Only Two Ways of Formatting Functions/Methods

Prefer function/method definitions with arguments in a single line. If it's too wide, put each argument on a new line.

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func function(argument1 int, argument2 string, argument3 time.Duration,
    argument4 someType, argument5 float64, argument6 time.Time) (ret int, err error) {
```

</td><td>

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

#### Control Structure: Avoid `else`

In most of the cases you don't need `else`. You can usually use `continue`, `break` or `return` to end an `if` block.
This enables having one less indent and netter consistency so code is more readable.

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
for _, elem := range elems {
    if a == 1 {
        something[i] = "yes"
    } else {
        something[i] = "no"
    }
}
```

</td><td>

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

#### Avoid Globals

No globals other than `const` are allowed. Period.
This means also, no `init` functions.

#### Prints.

Never use `print`. Always use a passed `go-kit/log.Logger`.

#### Use Named Return Parameters Carefully

It's OK to name return parameters if the types do not give enough information about what function or method actually returns.
Another use case is when you want to define a variable, e.g. a slice.

**IMPORTANT:** never use naked `return` statements with named return perameters. This compiles but it makes returning values
implicit and thus more prone to surprises.

#### Explicitly Handled Return Errors

Always address returned errors. It does not mean you cannot "ignore" the error for some reason, e.g. if we know implementation
will not return anything meaningful. You can ignore the error, but do so explicitly:

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
`someMethodThatReturnsError(...)`
```

</td><td>

```go
`_ = someMethodThatReturnsError(...)`
```

</td></tr>
</tbody></table>

The exception: well known cases such as `level.Debug|Warn` etc and `fmt.FPrint*`

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
// Use `CloseWithErrCapture` if you want to close and fail the function or method on a `f.Close` error (make sure thr `error` 
// return argument is named as `err`). If the error is already present, `CloseWithErrCapture` will append (not wrap)
// the `f.Close` error if any. 
defer runutil.CloseWithErrCapture(&err, f, "close file")

// Use `CloseWithLogOnErr` if you want to close and log error on `Warn` level on a `f.Close` error.
defer runutil.CloseWithLogOnErr(logger, f, "close file")
```

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func writeToFile(...) error {
    f, err := os.Open(...)
    if err != nil {
        return err
    }
    defer f.Close() // What if an error occurs here?
    
    // Write something to file... etc.
    return nil
}
```

</td><td>

```go
func writeToFile(...) (err error) {
    f, err := os.Open(...)
    if err != nil {
        return err
    }
    defer runutil.CloseWithErrCapture(&err, f, "close file") // All handled well.
    
    // Write something to file... etc.
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
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
resp, err := http.Get("http://example.com/")
if err != nil {
    // handle...
}
defer runutil.CloseWithLogOnErr(logger, resp.Body, "close response") // All good?

scanner := bufio.NewScanner(resp.Body)
for scanner.Scan() {
   // If any error happens and we return in the middle of scanning body, we can end up with unread buffer, which
   // will use memory and hold TCP connection!
```

</td><td>

```go
resp, err := http.Get("http://example.com/")
if err != nil {
    // handle...
}
defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "close response")

scanner := bufio.NewScanner(resp.Body)
for scanner.Scan() {
   // If any error happens and we return in the middle of scanning body, defer will handle all well.
```

</td></tr>
</tbody></table>

#### Wrap Errors for More Context; Don't Repeat "failed ..." There.

We use [`pkg/errors`](https://github.com/pkg/errors) package for `errors`. We prefer it over standard wrapping with `fmt.Sprintf` + `%w`,
as `errors.Wrap` is explicit. It's easy to by accident replace `%w` with `%v` or to add extra inconsistent characters to the string.    

Use [`pkg/errors.Wrap`](https://github.com/pkg/errors) to wrap errors for future context when errors occurs. It's recommended
to add more interesting variables to add context using `errors.Wrapf`, e.g. file names, IDs or things that fail, etc.

NOTE: never prefix wrap messages with wording like `failed ... ` or `error occured while...`. Just describe what we
wanted to do when the failure occurred. Those prefixes are just noise. We are wrapping error, so it's obvious that some error
occurred, right? (: Improve readability and consider avoiding those.

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
if err != nil {
    return errors.Wrapf(err, "error occurred while reading from file %s", f.Name)
}
```

</td><td>

```go
if err != nil {
    return errors.Wrapf(err, "read file %s", f.Name)
}
```

</td></tr>
</tbody></table>

#### Never Use Panics

Never use them. If some dependency use it, use [recover](https://golang.org/doc/effective_go.html#recover). Also consider
avoiding that dependency. 🙈

#### Pre-allocating Slices and Maps

Try to always preallocate slices and map either via `cap` or `length`. If you know the number elements you want to put
apriori, use that knowledge!  This significantly improves the latency of such code. Consider this as micro optimization,
however it's a good pattern to do it always, as it does not add much complexity. Performance wise, it's only relevant for critical,
code paths with big arrays.

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
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

</td><td>

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

// TODO example

#### Avoid Using the `reflect` or `unsafe` Packages

Use those only for very specific, critical cases. Especially `reflect` tend to be be very slow. For testing code it's fine to use reflect.

#### Use the Blank Identifier `_`

Black identifiers are very useful to mark variables that are not used. Consider the following cases:

```go
a, _, err := function1(...) // We don't need the second return parameter; let's use the blank identifier instead.
```

```go
var _ InterfaceA = TypeA // We don't need to use this variable, we just want to make sure TypeA implements InterfaceA.
```

```go
func (t *Type) SomeMethod(_ context.Context, abc int) error { // We don't use context argument; let's use the blank identifier to make it clear.
```

#### Tests for Packages / Structs That Involve `time` package.

Avoid unit testing based on real time. Always try to mock time that is used within struct by using for example `timeNow func() time.Time` field.
For production code, you can initialize the field with `time.Now`. For test code, you can set a custom time that will be used by the struct.

<table>
<thead><tr><th>🔥</th><th>🤓</th></tr></thead>
<tbody>
<tr><td>

```go
func (s *SomeType) IsExpired(created time.Time) bool {
    // Code is hardly testable.
    return time.Since(created) >= s.expiryDuration
} 
```

</td><td>

```go
func (s *SomeType) IsExpired(created time.Time) bool {
    // s.timeNow is time.Now on production, mocked in tests.
    return created.Add(s.expiryDuration).After(s.timeNow())
} 
```

</td></tr>
</tbody></table>

### Ensured by linters

## Bash

Overall try to NOT use bash. For scripts longer than 30 lines, consider writing it in Go as we did [here](https://github.com/thanos-io/thanos/blob/55cb8ca38b3539381dc6a781e637df15c694e50a/scripts/copyright/copyright.go).

If you have to, we follow the Google Shell style guide: https://google.github.io/styleguide/shellguide.html
