# github.com/thanos-io/thanos/pkg/errors

Robust, minimalistic `errors` primitives. All you need for reliable error handling for backend Go applications and libraries.

A good, default and popular error package **must** have small and consistent API that is powerful enough for most of the users.
Learned from experience from using and contributing to the most popular libraries [errors](https://golang.org/pkg/errors/)
and [pkg/error](https://github.com/pkg/errors), we decided to start the one we were dreaming about!🦄

## Goals

* Simple is better. No confusing API or too many ways of doing the same thing as other packages (including standard package!).
Less equals safer and cleaner.
* Compatible with standard Go [errors](https://golang.org/pkg/errors/).
* Only three features. All you really need!
  * Type safe, consistent error wrapping, with ability to add more context to the error.
  * Consistent and ONLY one way to find cause of error.
  * Optional stack trace report at the printout. 0 overhead if you don't need it.

## Usage

The traditional error handling idiom in Go looks roughly like this:

```go
if err != nil {
    return err
}
```

## Rationales

Designing APIs is difficult, especially for probably the most used package in Go language. Potentially it's even impossible,
as one-fits-all-sizes solutions rarely works out.

As many good ideas, this one [were born from the pain](https://github.com/thanos-io/thanos/pull/2359#discussion_r403945260) of using existing solutions.


* Lack of type safety and prone to inconsistency wrapping; Abuse of formatting package.
* Lack of essential feature.
* Confusing standard package, resulted from too wide API (too many way of doing the same thing)
* Overcomplicated stack trace.

Contrary: https://peter.bourgon.org/blog/2019/09/11/programming-with-errors.html

```go
package main

import (
	"errors"
	"fmt"
	pe "github.com/pkg/errors"
)

var (
	ErrorA = errors.New("error a")
	ErrorB = errors.New("error b")
)

func main() {
	err := B(A("my error message"))

	fmt.Println(err)
	fmt.Println(errors.Is(err, ErrorA))
	fmt.Println(errors.Is(err, ErrorB))

	err = errors.Unwrap(err)

	fmt.Println(err)
	fmt.Println(errors.Is(err, ErrorA))
	fmt.Println(errors.Is(err, ErrorB))

	/*err = C()

	fmt.Println(err)
	fmt.Println(errors.Is(err, ErrorA))
	fmt.Println(errors.Is(err, ErrorB))*/
}

func A(msg string) error {
	return fmt.Errorf("%w: %v", ErrorA, msg)
}

func B(err error) error {
	return fmt.Errorf("%w: %v", ErrorB, err)
}

func C() error {
	return pe.Wrap(ErrorA, ErrorB.Error())
}
```
* pkg/err confusing as well

WithStack vs Wrap vs WithMessage wtf? New etc.

* Important decisions are made every minor release; Resulted of feature creep.
* Minor: New vs Errorf and Wrap vs Wrapf.

* If err is nil, Wrap returns New error. NOT NIL!

THIS was buggy as hell 

* BUGs: pkg/error unhandled edge cases: fmt.Printf("%d", errors.Wrap(errors.New("some err"), "context")) will print nothing(!) 