package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/benchmark/stats"
)

func main() {
	fmt.Println(run())
}

func run() error {
	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}
	defer gcsClient.Close()

	bkt := gcsClient.Bucket("promlts-test")
	ctx := context.Background()

	objs := bkt.Objects(ctx, nil)

	var mtx sync.Mutex
	var wg sync.WaitGroup

	hist := stats.NewHistogram(stats.HistogramOptions{
		BaseBucketSize: 10,
		GrowthFactor:   0.5,
		NumBuckets:     12,
	})

	for {
		oa, err := objs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		o := bkt.Object(oa.Name)

		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 3000; j++ {
				dur, err := loadRange(oa.Name, o, oa.Size)
				if err == errSkip {
					continue
				}
				if err != nil {
					panic(err)
				}
				mtx.Lock()
				hist.Add(int64(dur / time.Millisecond))
				mtx.Unlock()
			}
		}()
	}
	wg.Wait()

	hist.Print(os.Stdout)
	return nil
}

var errSkip = errors.New("skip")

func loadRange(name string, o *storage.ObjectHandle, sz int64) (time.Duration, error) {
	lsz := rand.Int63n(16000)
	if lsz > sz {
		return 0, errSkip
	}
	start := rand.Int63n(sz - lsz)

	// fmt.Printf("%s: load %d bytes at %d\n", name, lsz, start)
	begin := time.Now()

	r, err := o.NewRangeReader(context.Background(), start, lsz)
	if err != nil {
		return 0, err
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	fmt.Println("took", time.Since(begin), len(b))

	return time.Since(begin), nil
}
