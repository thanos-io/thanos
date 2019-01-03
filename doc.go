/*
Package thanos is a set of components that
can provide highly available Prometheus
setup with long term storage capabilities.

See https://github.com/improbable-eng/thanos/blob/master/docs/getting_started.md for getting start.

Configuration watches and reloads

While you want to watch change event of the specified config files
and trigger reloads by reload url,
the way to use the feature in Thanos is simply by importing package pkg/reloader.
The main features are:

	* The given config files or directories can be watched and trigger reloads upon changes.
	* The given config files or directories will be checked as scheduled, it also will trigger reloads if changed.
	* It can substitute environment variables in the given config file and write into the specified path.
	* Retry trigger reloads until it succeeded or next tick is near.

Let's start creating a new reloader:

	func main() {
		rl := reloader.New(
			log.NewLogfmtLogger(os.Stdout),
			reloader.ReloadURLFromBase("http://localhost:9090"),
			"/path/to/cfg",
			"/path/to/cfg.out",
			"/path/to/dirs",
		)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			if err := rl.Watch(ctx); err != nil {
				log.Fatal(err)
			}
		}()

		reloadHandler := func(w http.ResponseWriter, req *http.Request) {
			io.WriteString(w, "Reloaded\n")
		}

		http.HandleFunc("/-/reload", reloadHandler)
		log.Fatal(http.ListenAndServe(":9090", nil))
	}

Here we create a new reloader to ready for watching.
If "/path/to/cfg.out" is not empty the config file will be decompressed if needed,
environment variables will be substituted and the output written into the given path.
The environment variables must be of the form "$(var)".

	rl := reloader.New(
		log.NewLogfmtLogger(os.Stdout),
		reloader.ReloadURLFromBase("http://localhost:9090"),
		"/path/to/cfg",
		"/path/to/cfg_out",
		"/path/to/dirs",
	)

The url of reloads can be generated with function ReloadURLFromBase().
It will append the default path of reload into the given url:

	reloader.ReloadURLFromBase("http://localhost:9090") // It will return 'http://localhost:9090/-/reload'

Start watching changes and stopped until the context gets canceled:

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := rl.Watch(ctx); err != nil {
			log.Fatal(err)
		}
	}()

By default, reloader will make a schedule to check the given config files and dirs of sum of hash with the last result,
even if it is no changes.

A basic example of configuration template with environment variables:

  global:
    external_labels:
      replica: '$(HOSTNAME)'

Utility functions for runtime

While meeting a scenario to do something every fixed intervals or be retried automatically,
it's a common thinking to use time.Ticker or for-loop. Here we provide easy ways to implement those by closure function.

For repeat executes, use Repeat:

	// ...
	stopc := make(chan struct{})
	defer close(stopc)

	err := runutil.Repeat(10*time.Second, stopc, func() error {
		fmt.Println("Repeat")
		return nil
	})
	// ...

Retry starts executing closure function f until no error is returned from f:

	// ...
	stopc := make(chan struct{})
	defer close(stopc)

	err := runutil.Retry(10*time.Second, stopc, func() error {
		fmt.Println("Retry")
		return errors.New("Try to retry.")
	})
	// ...

For logging an error on each f error, use RetryWithLog:

	// ...
	logger := log.NewLogfmtLogger(os.Stdout)
	stopc := make(chan struct{})
	defer close(stopc)

	err := runutil.RetryWithLog(logger, 10*time.Second, stopc, func() error {
		fmt.Println("RetryWithLog")
		return errors.New("Try to retry.")
	})
	// ...

As we all know, we should close all implements of io.Closer in Golang. Commonly we will use:

	defer closer.Close()

The Close() will return error if existed, sometimes we will ignore it. Thanos provides utility functions to log every error like those:

	defer runutil.CloseWithLogOnErr(logger, closer, "log format")

For capturing error, use CloseWithErrCapture:

	var err error
	defer runutil.CloseWithErrCapture(logger, &err, closer, "log format")

	// ...

If Close() returns error, err will capture it and return by argument.

*/
package thanos // import "github.com/improbable-eng/thanos"
