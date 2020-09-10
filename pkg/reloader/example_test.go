// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/thanos-io/thanos/pkg/reloader"
)

func ExampleReloader() {
	u, err := url.Parse("http://localhost:9090")
	if err != nil {
		log.Fatal(err)
	}
	rl := reloader.New(nil, nil, &reloader.Options{
		ReloadURL:     reloader.ReloadURLFromBase(u),
		CfgFile:       "/path/to/cfg",
		CfgOutputFile: "/path/to/cfg.out",
		WatchedDirs:   []string{"/path/to/dirs"},
		WatchInterval: 3 * time.Minute,
		RetryInterval: 5 * time.Second,
		DelayInterval: 1 * time.Second,
	})

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
	//    Output: http://localhost:9090/-/reload
}
