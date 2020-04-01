---
title: Coding Style guide
type: docs
menu: contributing
---

# Thanos Coding Style Guide.

This document explains the official style guides for various languages we use in the Thanos project.
Feel free to familiarize and refer this during code reviews. If something in our codebase does not match the style, it means it
was missed or it was done before this document. Help wanted to fix it! (:

Generally we care for:

* Readability, so low [Cognitive Load](https://www.dabapps.com/blog/cognitive-load-programming/).
* Maintainability. We avoid the code that **surprises**.
* Performance only for critical path and without compromising readability.
* Testability. Even if it means some changes to the production code, like `timeNow func() time.Time` mock.
* Consistency: If some pattern repeats, it means less surprises.

NOTE: Some style is ensured by our linters. This is not covered by this document. We only cover things that we did not setup linters yet for (:

## Go

For code written in [Go](https://golang.org/) we use standard Go style guides ([1](https://golang.org/doc/effective_go.html),[2](https://github.com/golang/go/wiki/CodeReviewComments)) with few additional rules that
makes certain areas more strict than standard guide.

### Only two ways of formatting functions/methods

Prefer either one line function/method definition arguments. If it's too wide, split it in following way:

```go
func function(
   a int,
   b string,
   c int,
) (ret int, err error)
```

### Control Structure: Avoid else

In most of the cases you don't need `else`. Usually you can use `continue`, `break` or `return` to end `if` body.
This allows to have one less indent and consistency, so code is more readable. Consider following example:

```go
for _, elem := range elems {
    if a == 1 {
        something[i] = "yes"
    } else {
    	something[i] = "no"
    }
}

```

We prefer following alternative without `else`:

```go
for _, elem := range elems {
    if a == 1 {
        something[i] = "yes"
        continue
    }

    something[i] = "no"
}

```

### Avoid globals

No globals other than `const` are allowed. Period.

Never use `init` functions.

### Prints.

Never use `print`. Always use passed `go-kit/log.Logger`

### Use name result/return parameters carefully

It's ok to name return parameters if the types are not giving the details what function actually returns. Another use case
is when you want to define variable (e.g slice).

**IMPORTANT:** Never use just "unadorned" `return`. This makes it's implicit what values are actually returned, thus more prone
to surprises.

### Explicitly handled return errors

Always address returned errors. It does not mean you cannot "ignore" err for some reason (e.g if we know implementation
will not return anything meaningful). You can, but do it explicitly:

`_ = someMethodThatReturnsError`

The exception: Well known cases like this like `level.Debug|Warn` etc and `fmt.FPrint*`

### Defers: Dont' forget to check returned errors

It's easy to forget about checking the error of some Close method that we defer e.g:

```go
f, err := os.Open(...)
if err != nil {
    // handle..
}

// do...

defer f.Close() // What if error happens here?

```

Not checked error like this can lead to major bugs. Consider above example, the `*os.File` Close can be responsible
for actually flush to the file, so if error happens at that point, the whole write might be aborted.

Always check error using our [runutil](https://pkg.go.dev/github.com/thanos-io/thanos@v0.11.0/pkg/runutil?tab=doc) helper package e.g:

If you want to error function on close error (make sure `error` return argument is named as `err`):

```go
defer runutil.CloseWithErrCapture(&err, f, "close file")
```

If you want to just log on error:

```go
defer runutil.CloseWithLogOnErr(logger, f, "close file")
```

### Exhaust readers

One of the most common bugs is forgetting to close or fully read bodies of http requests and responses, especially on
error. If you read body of such structures, you can use [runutil](https://pkg.go.dev/github.com/thanos-io/thanos@v0.11.0/pkg/runutil?tab=doc)
helper as well:

```go
defer runutil.ExhaustCloseWithLogOnErr(logger, resp.Body, "close file")
```

### Wrap errors for more context. Don't repeat "failed ..." there.

We use https://github.com/pkg/errors for errors. Use it to wrap errors for future context when errors happens. It's recommended
to add more interesting variables like e.g file names, id of things that fails.

NOTE: Never use `failed ... ` prefix for the wrap messages. Just describe what we wanted to do when fails. Consider following example:

```go
if err != nil {
    return errors.Wrapf(err, "error while reading from file %s", f.Name)
}
```

This is very unnecessary context. We are wrapping error, so it's obvious that some error happened. Improve readability and consider
replacing above with this:

```go
if err != nil {
    return errors.Wrapf(err, "read file %s", f.Name)
}
```

### Never use panics

Never use them. If some dependency use it, use [recover](https://golang.org/doc/effective_go.html#recover).

### Pre-allocating slices and maps

Try to always preallocate slices and map either via cap or length. If you know the number elements you want to put into the
new slice or map use following code:

`make([]MyType, 0, len(elems))` and `make(map[string]MyType, len(elems))`. This significantly improves latency of such code.
Consider this as micro optimization. It's only relevant for critical code paths. In such cases you might want to think about
reusing arrays.

### Don't use reflect or unsafe packages

Use those only for very specific, critical cases.
For testing code it's fine to use reflect.

### Use blank identifier `_`

Black identifiers are very useful to mark variables that are not used. Consider following cases:

```go
a, _, err := function1(...) // We don't need second return parameter, let's use blank instead.
```

```go
var _ InterfaceA = TypeA // We don't need to use this variable, we just want to make sure TypeA implements InterfaceA.
```

```go
func (t *Type) SomeMethod(_ context.Context, abc int) error { // We don't use context argument, let's use blank identifier to make it clear.
```

### Tests for packages / structs that involve time package.

Avoid unit testing based on real time. Always try to mock time that is used within struct by using for example `timeNow func() time.Time` field.
For production code you can initialise it with `time.Now`. For test code you can set custom time that will be used by struct.

// TODO:
* log lowercase
* errors.Cause
* variable shadowing, package name shadowing
* 

### Ensured by linter:  ...
## Bash

Overall try to NOT use bash. For script that has more than 30 lines, consider writing it in Go as we did [here](https://github.com/thanos-io/thanos/blob/55cb8ca38b3539381dc6a781e637df15c694e50a/scripts/copyright/copyright.go).

If you have to, we follow Google Shell style guide: https://google.github.io/styleguide/shellguide.html

## Javascript

TBD

