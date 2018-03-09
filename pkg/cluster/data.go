package cluster

import "sync"

type data struct {
	mtx  sync.RWMutex
	data map[string]PeerState
}

func (d *data) Set(k string, v PeerState) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.data[k] = v
}

func (d *data) Del(k string) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	delete(d.data, k)
}

func (d *data) Get(k string) (PeerState, bool) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	p, ok := d.data[k]
	return p, ok
}

func (d *data) Data() map[string]PeerState {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	res := map[string]PeerState{}
	for k, v := range d.data {
		res[k] = v
	}

	return res
}
