package reloader_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/improbable-eng/thanos/pkg/reloader"
)

func ExampleReloader() {
	u, err := url.Parse("http://localhost:9090")
	if err != nil {
		log.Fatal(err)
	}
	rl := reloader.New(
		nil,
		reloader.ReloadURLFromBase(u),
		"/path/to/cfg",
		"/path/to/cfg.out",
		[]string{"/path/to/dirs"},
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := rl.Watch(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	reloadHandler := func(w http.ResponseWriter, req *http.Request) {
		if _, err := io.WriteString(w, "Reloaded\n"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	http.HandleFunc("/-/reload", reloadHandler)
	log.Fatal(http.ListenAndServe(":9090", nil))

	cancel()
}

func ExampleReloadURLFromBase() {
	u, err := url.Parse("http://localhost:9090")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reloader.ReloadURLFromBase(u))
	// Output: http://localhost:9090/-/reload
}
