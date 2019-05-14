package indexcache

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randRunes(n int, runes []rune) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

func benchmarkJSON(n int, b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "test-json-encode")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(tmpDir)) }()

	lbls := []labels.Labels{}
	for i := 0; i < n; i++ {
		lbls = append(lbls, labels.Labels{labels.Label{Name: randRunes(10, letterRunes), Value: randRunes(10, letterRunes)}})
	}

	block, err := testutil.CreateBlock(tmpDir, lbls, 100, 0, 1000, nil, 124)
	testutil.Ok(b, err)

	fn := filepath.Join(tmpDir, "index.cache.json")
	l := log.NewNopLogger()
	j := JSONCache{logger: l}

	testutil.Ok(b, j.WriteIndexCache(filepath.Join(tmpDir, block.String(), "index"), fn))

	version, symbols, lvals, postings, err := j.ReadIndexCache(fn)
	testutil.Ok(b, err)
	var _ = version
	var _ = lvals
	var _ = symbols
	var _ = postings
}

func benchmarkBinary(n int, b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "test-bin-encode")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(tmpDir)) }()

	lbls := []labels.Labels{}
	for i := 0; i < n; i++ {
		lbls = append(lbls, labels.Labels{labels.Label{Name: randRunes(10, letterRunes), Value: randRunes(10, letterRunes)}})
	}

	block, err := testutil.CreateBlock(tmpDir, lbls, 100, 0, 1000, nil, 124)
	testutil.Ok(b, err)

	fn := filepath.Join(tmpDir, "index.cache.dat")
	l := log.NewNopLogger()
	bCache := BinaryCache{logger: l}

	testutil.Ok(b, bCache.WriteIndexCache(filepath.Join(tmpDir, block.String(), "index"), fn))

	version, symbols, lvals, postings, err := bCache.ReadIndexCache(fn)
	testutil.Ok(b, err)
	var _ = version
	var _ = lvals
	var _ = symbols
	var _ = postings
}

func BenchmarkJSON1(b *testing.B)  { benchmarkJSON(1000, b) }
func BenchmarkJSON2(b *testing.B)  { benchmarkJSON(2000, b) }
func BenchmarkJSON3(b *testing.B)  { benchmarkJSON(3000, b) }
func BenchmarkJSON4(b *testing.B)  { benchmarkJSON(4000, b) }
func BenchmarkJSON10(b *testing.B) { benchmarkJSON(10000, b) }
func BenchmarkJSON20(b *testing.B) { benchmarkJSON(20000, b) }

func BenchmarkBinary1(b *testing.B)  { benchmarkBinary(1000, b) }
func BenchmarkBinary2(b *testing.B)  { benchmarkBinary(2000, b) }
func BenchmarkBinary3(b *testing.B)  { benchmarkBinary(3000, b) }
func BenchmarkBinary4(b *testing.B)  { benchmarkBinary(4000, b) }
func BenchmarkBinary10(b *testing.B) { benchmarkBinary(10000, b) }
func BenchmarkBinary20(b *testing.B) { benchmarkBinary(20000, b) }
