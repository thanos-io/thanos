// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
)

type noopIndexCache struct {
	postingsData []byte
}

func (n *noopIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	n.postingsData = v
}
func (n *noopIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	return nil, nil
}
func (n *noopIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {}
func (n *noopIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	return nil, nil
}

type readerCloser struct {
	*bytes.Buffer
}

func (r *readerCloser) Close() error {
	return nil
}

func TestStreamedPostingsReader(t *testing.T) {
	postingsData := []byte{}
	eb := encoding.Encbuf{B: postingsData}
	eb.PutBE32(3)

	eb.PutBE32(0)
	eb.PutBE32(1)
	eb.PutBE32(2)

	ic := &noopIndexCache{}
	srr, err := newStreamedPostingsReader(&readerCloser{Buffer: bytes.NewBuffer(eb.Get())}, ulid.MustNew(1, nil), labels.Label{}, ic, nil, nil)
	testutil.Ok(t, err)

	refs := []storage.SeriesRef{}

	for srr.Next() {
		refs = append(refs, srr.At())
	}

	testutil.Equals(t, []storage.SeriesRef{0, 1, 2}, refs)

	testutil.Ok(t, srr.Close())
	testutil.Assert(t, len(ic.postingsData) == 24, fmt.Sprintf("postings data length is not 24 (%d)", len(ic.postingsData)))

	cp, err := diffVarintSnappyStreamedDecode(ic.postingsData)
	testutil.Ok(t, err)

	decompressedRefs := []storage.SeriesRef{}

	for cp.Next() {
		decompressedRefs = append(decompressedRefs, cp.At())
	}

	testutil.Equals(t, refs, decompressedRefs)
}

func TestStreamedPostingsReaderEdge(t *testing.T) {
	fis, err := os.ReadDir("./testdata")
	testutil.Ok(t, err)

	for _, fi := range fis {
		fi := fi
		t.Run(fi.Name(), func(t *testing.T) {
			postingsData, err := os.ReadFile(fmt.Sprintf("./testdata/%s", fi.Name()))
			testutil.Ok(t, err)

			ic := &noopIndexCache{}
			srr, err := newStreamedPostingsReader(&readerCloser{Buffer: bytes.NewBuffer(postingsData)}, ulid.MustNew(1, nil), labels.Label{}, ic, nil, nil)
			testutil.Ok(t, err)

			refs := []storage.SeriesRef{}

			for srr.Next() {
				refs = append(refs, srr.At())
			}

			t.Log(len(refs))

			testutil.Ok(t, srr.Close())
			testutil.Ok(t, srr.Err())
			testutil.Equals(t, (len(postingsData)/4)-1, len(refs))

			cp, err := diffVarintSnappyStreamedDecode(ic.postingsData)
			testutil.Ok(t, err)

			decompressedRefs := []storage.SeriesRef{}

			for cp.Next() {
				decompressedRefs = append(decompressedRefs, cp.At())
			}

			testutil.Equals(t, refs, decompressedRefs)
		})
	}

	streamPostingsReaders := []index.Postings{}
	for _, fi := range fis {
		fi := fi
		postingsData, err := os.ReadFile(fmt.Sprintf("./testdata/%s", fi.Name()))
		testutil.Ok(t, err)

		ic := &noopIndexCache{}
		srr, err := newStreamedPostingsReader(&readerCloser{Buffer: bytes.NewBuffer(postingsData)}, ulid.MustNew(1, nil), labels.Label{}, ic, nil, nil)
		testutil.Ok(t, err)

		streamPostingsReaders = append(streamPostingsReaders, srr)
	}

	intersected := index.Without(index.Intersect(streamPostingsReaders...), index.EmptyPostings())
	refs := []storage.SeriesRef{}

	for intersected.Next() {
		refs = append(refs, intersected.At())
	}

	t.Log(len(refs))
}
