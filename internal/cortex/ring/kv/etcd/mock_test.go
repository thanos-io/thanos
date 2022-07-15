package etcd

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Tests to make sure the mock Etcd client works which we need for kv.Client tests.
// Quis custodiet ipsos custodes?

func TestMockKv_Get(t *testing.T) {
	t.Run("exact match", func(t *testing.T) {
		pair := mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}

		kv := newMockKV()
		kv.values = map[string]mvccpb.KeyValue{string(pair.Key): pair}
		res, err := kv.Get(context.Background(), "/foo")

		require.NoError(t, err)
		require.Len(t, res.Kvs, 1)
		assert.Equal(t, []byte("/foo"), res.Kvs[0].Key)
	})

	t.Run("not exact match", func(t *testing.T) {
		pair := mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}

		kv := newMockKV()
		kv.values = map[string]mvccpb.KeyValue{string(pair.Key): pair}
		res, err := kv.Get(context.Background(), "/bar")

		require.NoError(t, err)
		assert.Empty(t, res.Kvs)
	})

	t.Run("prefix match", func(t *testing.T) {
		fooPair := mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}
		bazPair := mvccpb.KeyValue{
			Key:   []byte("/baz"),
			Value: []byte("2"),
		}
		firstPair := mvccpb.KeyValue{
			Key:   []byte("/first"),
			Value: []byte("3"),
		}

		kv := newMockKV()
		kv.values = map[string]mvccpb.KeyValue{
			string(fooPair.Key):   fooPair,
			string(bazPair.Key):   bazPair,
			string(firstPair.Key): firstPair,
		}
		res, err := kv.Get(context.Background(), "/f", clientv3.WithPrefix())

		require.NoError(t, err)
		assert.ElementsMatch(t, []*mvccpb.KeyValue{&fooPair, &firstPair}, res.Kvs)
	})

	t.Run("empty prefix", func(t *testing.T) {
		fooPair := mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}
		bazPair := mvccpb.KeyValue{
			Key:   []byte("/baz"),
			Value: []byte("2"),
		}
		firstPair := mvccpb.KeyValue{
			Key:   []byte("/first"),
			Value: []byte("3"),
		}

		kv := newMockKV()
		kv.values = map[string]mvccpb.KeyValue{
			string(fooPair.Key):   fooPair,
			string(bazPair.Key):   bazPair,
			string(firstPair.Key): firstPair,
		}
		res, err := kv.Get(context.Background(), "", clientv3.WithPrefix())

		require.NoError(t, err)
		assert.ElementsMatch(t, []*mvccpb.KeyValue{&fooPair, &bazPair, &firstPair}, res.Kvs)
	})
}

func TestMockKV_Put(t *testing.T) {
	t.Run("new key", func(t *testing.T) {
		kv := newMockKV()
		_, err := kv.Put(context.Background(), "/foo", "1")

		require.NoError(t, err)
		assert.Equal(t, int64(1), kv.values["/foo"].Version)
		assert.Equal(t, []byte("1"), kv.values["/foo"].Value)
	})

	t.Run("existing key", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:            []byte("/foo"),
			CreateRevision: 1,
			ModRevision:    2,
			Version:        2,
			Value:          []byte("1"),
		}

		_, err := kv.Put(context.Background(), "/foo", "2")

		require.NoError(t, err)
		assert.Equal(t, int64(3), kv.values["/foo"].Version)
		assert.Equal(t, []byte("2"), kv.values["/foo"].Value)
	})
}

func TestMockKV_Delete(t *testing.T) {
	t.Run("exact match", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}

		res, err := kv.Delete(context.Background(), "/foo")

		require.NoError(t, err)
		assert.Equal(t, int64(1), res.Deleted)
		assert.Empty(t, kv.values)
	})

	t.Run("prefix match", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}
		kv.values["/baz"] = mvccpb.KeyValue{
			Key:   []byte("/baz"),
			Value: []byte("2"),
		}
		kv.values["/first"] = mvccpb.KeyValue{
			Key:   []byte("/first"),
			Value: []byte("3"),
		}

		res, err := kv.Delete(context.Background(), "/f", clientv3.WithPrefix())

		require.NoError(t, err)
		assert.Equal(t, int64(2), res.Deleted)
		assert.NotContains(t, kv.values, "/foo")
		assert.NotContains(t, kv.values, "/first")
	})

	t.Run("empty prefix", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:   []byte("/foo"),
			Value: []byte("1"),
		}
		kv.values["/baz"] = mvccpb.KeyValue{
			Key:   []byte("/baz"),
			Value: []byte("2"),
		}
		kv.values["/first"] = mvccpb.KeyValue{
			Key:   []byte("/first"),
			Value: []byte("3"),
		}

		res, err := kv.Delete(context.Background(), "", clientv3.WithPrefix())

		require.NoError(t, err)
		assert.Equal(t, int64(3), res.Deleted)
		assert.NotContains(t, kv.values, "/foo")
		assert.NotContains(t, kv.values, "/baz")
		assert.NotContains(t, kv.values, "/first")
	})
}

func TestMockKV_Txn(t *testing.T) {
	t.Run("success compare value", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:            []byte("/foo"),
			CreateRevision: 1,
			ModRevision:    3,
			Version:        3,
			Value:          []byte("1"),
		}

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Value("/foo"), "=", "1")).
			Then(clientv3.OpPut("/foo", "2")).
			Commit()

		require.NoError(t, err)
		assert.True(t, res.Succeeded)
		assert.Equal(t, kv.values["/foo"].Value, []byte("2"))
	})

	t.Run("failure compare value", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:            []byte("/foo"),
			CreateRevision: 1,
			ModRevision:    3,
			Version:        3,
			Value:          []byte("2"),
		}

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Value("/foo"), "=", "3")).
			Then(clientv3.OpPut("/foo", "4")).
			Else(clientv3.OpPut("/foo", "-1")).
			Commit()

		require.NoError(t, err)
		assert.False(t, res.Succeeded)
		assert.Equal(t, kv.values["/foo"].Value, []byte("-1"))
	})

	t.Run("success compare version exists", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:            []byte("/foo"),
			CreateRevision: 1,
			ModRevision:    3,
			Version:        3,
			Value:          []byte("1"),
		}

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version("/foo"), "=", 3)).
			Then(clientv3.OpPut("/foo", "2")).
			Commit()

		require.NoError(t, err)
		assert.True(t, res.Succeeded)
		assert.Equal(t, kv.values["/foo"].Value, []byte("2"))
	})

	t.Run("failure compare version exists", func(t *testing.T) {
		kv := newMockKV()
		kv.values["/foo"] = mvccpb.KeyValue{
			Key:            []byte("/foo"),
			CreateRevision: 1,
			ModRevision:    3,
			Version:        3,
			Value:          []byte("1"),
		}

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version("/foo"), "=", 2)).
			Then(clientv3.OpPut("/foo", "2")).
			Commit()

		require.NoError(t, err)
		assert.False(t, res.Succeeded)
		assert.Equal(t, kv.values["/foo"].Value, []byte("1"))
	})

	t.Run("success compare version does not exist", func(t *testing.T) {
		kv := newMockKV()

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version("/foo"), "=", 0)).
			Then(clientv3.OpPut("/foo", "1")).
			Commit()

		require.NoError(t, err)
		assert.True(t, res.Succeeded)
		assert.Equal(t, kv.values["/foo"].Value, []byte("1"))
	})

	t.Run("failure compare version does not exist", func(t *testing.T) {
		kv := newMockKV()

		res, err := kv.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version("/foo"), "=", 1)).
			Then(clientv3.OpPut("/foo", "2")).
			Commit()

		_, ok := kv.values["/foo"]

		require.NoError(t, err)
		assert.False(t, res.Succeeded)
		assert.False(t, ok)
	})
}

func TestMockKV_Watch(t *testing.T) {
	// setupWatchTest spawns a goroutine to watch for events matching a particular key
	// emitted by a mockKV. Any observed events are sent to callers via the returned channel.
	// The goroutine can be stopped using the return cancel function and waited for using the
	// returned wait group.
	setupWatchTest := func(key string, prefix bool) (*mockKV, context.CancelFunc, chan *clientv3.Event, *sync.WaitGroup) {
		kv := newMockKV()
		// Use a condition to make sure the goroutine has started using the watch before
		// we do anything to the mockKV that would emit an event the watcher is expecting
		cond := sync.NewCond(&sync.Mutex{})
		wg := sync.WaitGroup{}
		ch := make(chan *clientv3.Event)
		ctx, cancel := context.WithCancel(context.Background())

		wg.Add(1)
		go func() {
			defer wg.Done()

			var ops []clientv3.OpOption
			if prefix {
				ops = []clientv3.OpOption{clientv3.WithPrefix()}
			}

			watch := kv.Watch(ctx, key, ops...)
			cond.Broadcast()

			for e := range watch {
				if len(e.Events) > 0 {
					ch <- e.Events[0]
				}
			}
		}()

		// Wait for the watcher goroutine to start actually watching
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()

		return kv, cancel, ch, &wg
	}

	t.Run("watch stopped by context", func(t *testing.T) {
		// Ensure we can use the cancel method of the context given to the watch
		// to stop the watch
		_, cancel, _, wg := setupWatchTest("/bar", false)
		cancel()
		wg.Wait()
	})

	t.Run("watch stopped by close", func(t *testing.T) {
		// Ensure we can use the Close method of the mockKV given to the watch
		// to stop the watch
		kv, _, _, wg := setupWatchTest("/bar", false)
		_ = kv.Close()
		wg.Wait()
	})

	t.Run("watch exact key", func(t *testing.T) {
		// watch for events with key "/bar" and send them via the channel
		kv, cancel, ch, wg := setupWatchTest("/bar", false)

		_, err := kv.Put(context.Background(), "/foo", "1")
		require.NoError(t, err)

		_, err = kv.Put(context.Background(), "/bar", "1")
		require.NoError(t, err)

		event := <-ch
		assert.Equal(t, mvccpb.PUT, event.Type)
		assert.Equal(t, []byte("/bar"), event.Kv.Key)

		cancel()
		wg.Wait()
	})

	t.Run("watch prefix match", func(t *testing.T) {
		// watch for events with the prefix "/b" and send them via the channel
		kv, cancel, ch, wg := setupWatchTest("/b", true)

		_, err := kv.Delete(context.Background(), "/foo")
		require.NoError(t, err)

		_, err = kv.Put(context.Background(), "/bar", "1")
		require.NoError(t, err)

		event := <-ch
		assert.Equal(t, mvccpb.PUT, event.Type)
		assert.Equal(t, []byte("/bar"), event.Kv.Key)

		cancel()
		wg.Wait()
	})
}
