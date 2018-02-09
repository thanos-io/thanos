// Copyright 2016 The Oklog Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ulid_test

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/oklog/ulid"
)

func ExampleULID() {
	t := time.Unix(1000000, 0)
	entropy := rand.New(rand.NewSource(t.UnixNano()))
	fmt.Println(ulid.MustNew(ulid.Timestamp(t), entropy))
	// Output: 0000XSNJG0MQJHBF4QX1EFD6Y3
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("ULID", testULID(func(ms uint64, e io.Reader) ulid.ULID {
		id, err := ulid.New(ms, e)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}))

	t.Run("Error", func(t *testing.T) {
		_, err := ulid.New(ulid.MaxTime()+1, nil)
		if got, want := err, ulid.ErrBigTime; got != want {
			t.Errorf("got err %v, want %v", got, want)
		}

		_, err = ulid.New(0, strings.NewReader(""))
		if got, want := err, io.EOF; got != want {
			t.Errorf("got err %v, want %v", got, want)
		}
	})
}

func TestMustNew(t *testing.T) {
	t.Parallel()

	t.Run("ULID", testULID(ulid.MustNew))

	t.Run("Panic", func(t *testing.T) {
		defer func() {
			if got, want := recover(), io.EOF; got != want {
				t.Errorf("panic with err %v, want %v", got, want)
			}
		}()
		_ = ulid.MustNew(0, strings.NewReader(""))
	})
}

func TestMustParse(t *testing.T) {
	t.Parallel()

	defer func() {
		if got, want := recover(), ulid.ErrDataSize; got != want {
			t.Errorf("got panic %v, want %v", got, want)
		}
	}()

	_ = ulid.MustParse("")
}

func testULID(mk func(uint64, io.Reader) ulid.ULID) func(*testing.T) {
	return func(t *testing.T) {
		want := ulid.ULID{0x0, 0x0, 0x0, 0x1, 0x86, 0xa0}
		if got := mk(1e5, nil); got != want { // optional entropy
			t.Errorf("\ngot  %#v\nwant %#v", got, want)
		}

		entropy := bytes.Repeat([]byte{0xFF}, 16)
		copy(want[6:], entropy)
		if got := mk(1e5, bytes.NewReader(entropy)); got != want {
			t.Errorf("\ngot  %#v\nwant %#v", got, want)
		}
	}
}

func TestRoundTrips(t *testing.T) {
	t.Parallel()

	prop := func(id ulid.ULID) bool {
		bin, err := id.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		txt, err := id.MarshalText()
		if err != nil {
			t.Fatal(err)
		}

		var a ulid.ULID
		if err = a.UnmarshalBinary(bin); err != nil {
			t.Fatal(err)
		}

		var b ulid.ULID
		if err = b.UnmarshalText(txt); err != nil {
			t.Fatal(err)
		}

		return id == a && b == id &&
			id == ulid.MustParse(id.String())
	}

	err := quick.Check(prop, &quick.Config{MaxCount: 1E5})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMarshalingErrors(t *testing.T) {
	t.Parallel()

	var id ulid.ULID
	for _, tc := range []struct {
		name string
		fn   func([]byte) error
		err  error
	}{
		{"UnmarshalBinary", id.UnmarshalBinary, ulid.ErrDataSize},
		{"UnmarshalText", id.UnmarshalText, ulid.ErrDataSize},
		{"MarshalBinaryTo", id.MarshalBinaryTo, ulid.ErrBufferSize},
		{"MarshalTextTo", id.MarshalTextTo, ulid.ErrBufferSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := tc.fn([]byte{}), tc.err; got != want {
				t.Errorf("got err %v, want %v", got, want)
			}
		})

	}
}

func TestAlizainCompatibility(t *testing.T) {
	t.Parallel()

	ts := uint64(1469918176385)
	got := ulid.MustNew(ts, bytes.NewReader(make([]byte, 16)))
	want := ulid.MustParse("01ARYZ6S410000000000000000")
	if got != want {
		t.Fatalf("with time=%d, got %q, want %q", ts, got, want)
	}
}

func TestEncoding(t *testing.T) {
	t.Parallel()

	enc := make(map[rune]bool, len(ulid.Encoding))
	for _, r := range ulid.Encoding {
		enc[r] = true
	}

	prop := func(id ulid.ULID) bool {
		for _, r := range id.String() {
			if !enc[r] {
				return false
			}
		}
		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 1E5}); err != nil {
		t.Fatal(err)
	}
}

func TestLexicographicalOrder(t *testing.T) {
	t.Parallel()

	prop := func(a, b ulid.ULID) bool {
		t1, t2 := a.Time(), b.Time()
		s1, s2 := a.String(), b.String()
		ord := bytes.Compare(a[:], b[:])
		return t1 == t2 ||
			(t1 > t2 && s1 > s2 && ord == +1) ||
			(t1 < t2 && s1 < s2 && ord == -1)
	}

	top := ulid.MustNew(ulid.MaxTime(), nil)
	for i := 0; i < 10; i++ { // test upper boundary state space
		next := ulid.MustNew(top.Time()-1, nil)
		if !prop(top, next) {
			t.Fatalf("bad lexicographical order: (%v, %q) > (%v, %q) == false",
				top.Time(), top,
				next.Time(), next,
			)
		}
		top = next
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 1E6}); err != nil {
		t.Fatal(err)
	}
}

func TestCaseInsensitivity(t *testing.T) {
	t.Parallel()

	upper := func(id ulid.ULID) (out ulid.ULID) {
		return ulid.MustParse(strings.ToUpper(id.String()))
	}

	lower := func(id ulid.ULID) (out ulid.ULID) {
		return ulid.MustParse(strings.ToLower(id.String()))
	}

	err := quick.CheckEqual(upper, lower, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseRobustness(t *testing.T) {
	t.Parallel()

	cases := [][]byte{
		{0x1, 0xc0, 0x73, 0x62, 0x4a, 0xaf, 0x39, 0x78, 0x51, 0x4e, 0xf8, 0x44, 0x3b,
			0xb2, 0xa8, 0x59, 0xc7, 0x5f, 0xc3, 0xcc, 0x6a, 0xf2, 0x6d, 0x5a, 0xaa, 0x20},
	}

	for _, tc := range cases {
		if _, err := ulid.Parse(string(tc)); err != nil {
			t.Error(err)
		}
	}

	prop := func(s [26]byte) (ok bool) {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
				ok = false
			}
		}()

		var err error
		if _, err = ulid.Parse(string(s[:])); err != nil {
			t.Error(err)
		}

		return err == nil
	}

	err := quick.Check(prop, &quick.Config{MaxCount: 1E4})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNow(t *testing.T) {
	t.Parallel()

	before := ulid.Now()
	after := ulid.Timestamp(time.Now().UTC().Add(time.Millisecond))

	if before >= after {
		t.Fatalf("clock went mad: before %v, after %v", before, after)
	}
}

func TestTimestamp(t *testing.T) {
	t.Parallel()

	tm := time.Unix(1, 1000) // will be truncated
	if got, want := ulid.Timestamp(tm), uint64(1000); got != want {
		t.Errorf("for %v, got %v, want %v", tm, got, want)
	}

	mt := ulid.MaxTime()
	dt := time.Unix(int64(mt/1000), int64((mt%1000)*1000000)).Truncate(time.Millisecond)
	ts := ulid.Timestamp(dt)
	if got, want := ts, mt; got != want {
		t.Errorf("got timestamp %d, want %d", got, want)
	}
}

func TestTime(t *testing.T) {
	t.Parallel()

	maxTime := ulid.MaxTime()

	var id ulid.ULID
	if got, want := id.SetTime(maxTime+1), ulid.ErrBigTime; got != want {
		t.Errorf("got err %v, want %v", got, want)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 1e6; i++ {
		ms := uint64(rng.Int63n(int64(maxTime)))

		var id ulid.ULID
		if err := id.SetTime(ms); err != nil {
			t.Fatal(err)
		}

		if got, want := id.Time(), ms; got != want {
			t.Fatalf("\nfor %v:\ngot  %v\nwant %v", id, got, want)
		}
	}
}

func TestEntropy(t *testing.T) {
	t.Parallel()

	var id ulid.ULID
	if got, want := id.SetEntropy([]byte{}), ulid.ErrDataSize; got != want {
		t.Errorf("got err %v, want %v", got, want)
	}

	prop := func(e [10]byte) bool {
		var id ulid.ULID
		if err := id.SetEntropy(e[:]); err != nil {
			t.Fatalf("got err %v", err)
		}

		got, want := id.Entropy(), e[:]
		eq := bytes.Equal(got, want)
		if !eq {
			t.Errorf("\n(!= %v\n    %v)", got, want)
		}

		return eq
	}

	if err := quick.Check(prop, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()

	a := func(a, b ulid.ULID) int {
		return strings.Compare(a.String(), b.String())
	}

	b := func(a, b ulid.ULID) int {
		return a.Compare(b)
	}

	err := quick.CheckEqual(a, b, &quick.Config{MaxCount: 1E5})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkNew(b *testing.B) {
	b.Run("WithCryptoEntropy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = ulid.New(123, crand.Reader)
		}
	})

	b.Run("WithEntropy", func(b *testing.B) {
		now := time.Now().UTC()
		entropy := rand.New(rand.NewSource(now.UnixNano()))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = ulid.New(123, entropy)
		}
	})

	b.Run("WithoutEntropy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = ulid.New(123, nil)
		}
	})
}

func BenchmarkMustNew(b *testing.B) {
	b.Run("WithCryptoEntropy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ulid.MustNew(123, crand.Reader)
		}
	})

	b.Run("WithEntropy", func(b *testing.B) {
		now := time.Now().UTC()
		entropy := rand.New(rand.NewSource(now.UnixNano()))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = ulid.MustNew(123, entropy)
		}
	})

	b.Run("WithoutEntropy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ulid.MustNew(123, nil)
		}
	})
}

func BenchmarkParse(b *testing.B) {
	const s = "0000XSNJG0MQJHBF4QX1EFD6Y3"
	for i := 0; i < b.N; i++ {
		_, _ = ulid.Parse(s)
	}
}

func BenchmarkMustParse(b *testing.B) {
	const s = "0000XSNJG0MQJHBF4QX1EFD6Y3"
	for i := 0; i < b.N; i++ {
		_ = ulid.MustParse(s)
	}
}

func BenchmarkString(b *testing.B) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := ulid.MustNew(123456, entropy)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = id.String()
	}
}

func BenchmarkMarshal(b *testing.B) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, ulid.EncodedSize)
	id := ulid.MustNew(123456, entropy)

	b.Run("Text", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = id.MarshalText()
		}
	})

	b.Run("TextTo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = id.MarshalTextTo(buf)
		}
	})

	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = id.MarshalBinary()
		}
	})

	b.Run("BinaryTo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = id.MarshalBinaryTo(buf)
		}
	})
}

func BenchmarkUnmarshal(b *testing.B) {
	var id ulid.ULID
	s := "0000XSNJG0MQJHBF4QX1EFD6Y3"
	txt := []byte(s)
	bin, _ := ulid.MustParse(s).MarshalBinary()

	b.Run("Text", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = id.UnmarshalText(txt)
		}
	})

	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = id.UnmarshalBinary(bin)
		}
	})
}

func BenchmarkNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ulid.Now()
	}
}

func BenchmarkTimestamp(b *testing.B) {
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ulid.Timestamp(now)
	}
}

func BenchmarkTime(b *testing.B) {
	id := ulid.MustNew(123456789, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = id.Time()
	}
}

func BenchmarkSetTime(b *testing.B) {
	var id ulid.ULID
	for i := 0; i < b.N; i++ {
		_ = id.SetTime(123456789)
	}
}

func BenchmarkEntropy(b *testing.B) {
	id := ulid.MustNew(0, strings.NewReader("ABCDEFGHIJKLMNOP"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = id.Entropy()
	}
}

func BenchmarkSetEntropy(b *testing.B) {
	var id ulid.ULID
	e := []byte("ABCDEFGHIJKLMNOP")
	for i := 0; i < b.N; i++ {
		_ = id.SetEntropy(e)
	}
}
