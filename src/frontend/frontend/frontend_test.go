package frontend

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"router/router"
	"storage"
)

type MockRouter struct {
	nodesFind func(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error)
	list      func(router storage.ServiceAddr) ([]storage.ServiceAddr, error)
}

func (r *MockRouter) Heartbeat(router, node storage.ServiceAddr) error {
	return nil
}

func (r *MockRouter) NodesFind(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
	return r.nodesFind(router, k)
}

func (r *MockRouter) List(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
	return r.list(router)
}

type MockNode struct {
	put func(node storage.ServiceAddr, k storage.RecordID, d []byte) error
	get func(node storage.ServiceAddr, k storage.RecordID) ([]byte, error)
	del func(node storage.ServiceAddr, k storage.RecordID) error
}

func (n *MockNode) Put(node storage.ServiceAddr, k storage.RecordID, d []byte) error {
	return n.put(node, k, d)
}

func (n *MockNode) Get(node storage.ServiceAddr, k storage.RecordID) ([]byte, error) {
	return n.get(node, k)
}

func (n *MockNode) Del(node storage.ServiceAddr, k storage.RecordID) error {
	return n.del(node, k)
}

func nodesFind(t *testing.T, cfg Config, key storage.RecordID, nodes []storage.ServiceAddr, err error) func(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
	return func(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
		if router != cfg.Router {
			t.Errorf("Got %q, want %q", router, cfg.Router)
		}
		if k != key {
			t.Errorf("Got %v, want %v", k, key)
		}
		return nodes, err
	}
}

func get(t *testing.T, nodes []storage.ServiceAddr, key storage.RecordID, cb func(node storage.ServiceAddr) ([]byte, error)) func(storage.ServiceAddr, storage.RecordID) ([]byte, error) {
	var lock sync.Mutex
	set := make(map[storage.ServiceAddr]bool)
	for _, node := range nodes {
		set[node] = true
	}
	return func(node storage.ServiceAddr, k storage.RecordID) ([]byte, error) {
		if k != key {
			t.Errorf("Got %v, want %v", k, key)
		}
		lock.Lock()
		if !set[node] {
			t.Errorf("Got %v, exptected one of %v", node, nodes)
		}
		delete(set, node)
		lock.Unlock()
		if cb != nil {
			return cb(node)
		}
		return nil, nil
	}
}

func del(t *testing.T, nodes []storage.ServiceAddr, key storage.RecordID, cb func(storage.ServiceAddr) error) func(storage.ServiceAddr, storage.RecordID) error {
	f := get(t, nodes, key, func(node storage.ServiceAddr) ([]byte, error) {
		if cb == nil {
			return nil, nil
		}
		return nil, cb(node)
	})

	return func(node storage.ServiceAddr, k storage.RecordID) error {
		_, err := f(node, k)
		return err
	}
}

func put(t *testing.T, nodes []storage.ServiceAddr, key storage.RecordID, d []byte, cb func(storage.ServiceAddr) error) func(storage.ServiceAddr, storage.RecordID, []byte) error {
	f := del(t, nodes, key, cb)
	return func(node storage.ServiceAddr, k storage.RecordID, dd []byte) error {
		if !reflect.DeepEqual(dd, d) {
			t.Errorf("Got %s, want %s", dd, d)
		}
		return f(node, k)
	}
}

var rc MockRouter
var nc MockNode
var nf router.NodesFinder

var cfg = Config{
	NC:     &nc,
	RC:     &rc,
	NF:     nf,
	Router: "router",
}

func TestPutDel(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("testtesttest")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	for _, n := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprint("nodes=", n), func(t *testing.T) {
			rc.nodesFind = nodesFind(t, cfg, key, nodes[:n], nil)
			nc.put = put(t, nodes[:n], key, testData, nil)
			nc.del = del(t, nodes[:n], key, nil)
			fe := New(cfg)
			var wantError error
			if n < storage.MinRedundancy {
				wantError = storage.ErrNotEnoughDaemons
			}
			if err := fe.Put(key, testData); err != wantError {
				t.Errorf("Put() got error %v, want %v", err, wantError)
			}
			if err := fe.Del(key); err != wantError {
				t.Errorf("Del() got error %v, want %v", err, wantError)
			}
		})
	}
}

func TestPutDel_nodesFindError(t *testing.T) {
	key := storage.RecordID(1)
	errWant := errors.New("nodesFind dummy error")
	rc.nodesFind = nodesFind(t, cfg, key, nil, errWant)
	fe := New(cfg)
	if err := fe.Put(key, []byte("123")); err != errWant {
		t.Errorf("Put() got error %v, want %v", err, errWant)
	}
	if err := fe.Del(key); err != errWant {
		t.Errorf("Del() got error %v, want %v", err, errWant)
	}
}

func TestPutDel_Redundancy(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("testtesttest")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	errDummy := fmt.Errorf("dummy error")

	for _, test := range []struct {
		nodes  []storage.ServiceAddr
		errors map[storage.ServiceAddr]error
		err    error
	}{
		{
			nodes:  nodes[:2],
			errors: map[storage.ServiceAddr]error{nodes[0]: nil, nodes[1]: errDummy},
			err:    storage.ErrQuorumNotReached,
		},
		{
			nodes:  nodes[:2],
			errors: map[storage.ServiceAddr]error{nodes[0]: errors.New("err1"), nodes[1]: errors.New("err2")},
			err:    storage.ErrQuorumNotReached,
		},
		{
			nodes:  nodes,
			errors: map[storage.ServiceAddr]error{nodes[0]: nil, nodes[1]: errDummy, nodes[2]: errDummy},
			err:    errDummy,
		},
		{
			nodes:  nodes,
			errors: map[storage.ServiceAddr]error{nodes[0]: errors.New("err1"), nodes[1]: errors.New("err2"), nodes[2]: errors.New("err3")},
			err:    storage.ErrQuorumNotReached,
		},
	} {
		t.Run(fmt.Sprintf("nodes=%d,err=%v", len(test.nodes), test.err), func(t *testing.T) {
			rc.nodesFind = nodesFind(t, cfg, key, test.nodes, nil)
			nc.put = put(t, nodes, key, testData, func(node storage.ServiceAddr) error {
				return test.errors[node]
			})
			nc.del = del(t, nodes, key, func(node storage.ServiceAddr) error {
				return test.errors[node]
			})

			fe := New(cfg)
			if err := fe.Put(key, testData); err != test.err {
				t.Errorf("Put() got error %v, want %v", err, test.err)
			}
			if err := fe.Del(key); err != test.err {
				t.Errorf("Del() got error %v, want %v", err, test.err)
			}
		})
	}
}

func eqTime(a, b time.Duration) bool {
	const eps = 50 * time.Millisecond
	diff := a - b
	if diff < 0 {
		diff = b - a
	}
	return diff <= eps
}

func TestPutDel_Timing(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("testtesttest")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	rc.nodesFind = nodesFind(t, cfg, key, nodes, nil)
	sleep := 250 * time.Millisecond
	sleeps := map[storage.ServiceAddr]time.Duration{
		nodes[0]: sleep,
		nodes[1]: sleep * 2,
		nodes[2]: sleep * 3,
	}
	nc.put = put(t, nodes, key, testData, func(node storage.ServiceAddr) error {
		time.Sleep(sleeps[node])
		return nil
	})
	nc.del = del(t, nodes, key, func(node storage.ServiceAddr) error {
		time.Sleep(sleeps[node])
		return nil
	})

	fe := New(cfg)

	start := time.Now()
	if err := fe.Put(key, testData); err != nil {
		t.Errorf("Put() error  %v", err)
	}

	if diff := time.Since(start); !eqTime(diff, 3*sleep) {
		t.Errorf("Put() took too long")
	}

	start = time.Now()
	if err := fe.Del(key); err != nil {
		t.Errorf("Del() error  %v", err)
	}
	if diff := time.Since(start); !eqTime(diff, 3*sleep) {
		t.Errorf("Del() took too long")
	}
}

type FakeHasher struct {
	t      *testing.T
	hashes map[storage.ServiceAddr]uint64
}

func (h FakeHasher) Hash(k storage.RecordID, node storage.ServiceAddr) uint64 {
	hash, ok := h.hashes[node]
	if !ok {
		h.t.Fatalf("Unknown node %v", node)
	}

	return hash
}

func TestGet(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("test")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	rc.list = func(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
		return nodes, nil
	}

	dummyError := fmt.Errorf("dummy error")
	type resp struct {
		d   []byte
		err error
	}

	tests := []struct {
		name  string
		resps map[storage.ServiceAddr]resp
		err   error
	}{
		{
			name: "success",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{d: testData}, nodes[1]: resp{d: testData}, nodes[2]: resp{d: testData},
			},
		},
		{
			name: "one_error_first",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{err: dummyError}, nodes[1]: resp{d: testData}, nodes[2]: resp{d: testData},
			},
		},
		{
			name: "one_error_mid",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{d: testData}, nodes[1]: resp{err: dummyError}, nodes[2]: resp{d: testData},
			},
		},
		{
			name: "one_error_last",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{d: testData}, nodes[1]: resp{d: testData}, nodes[2]: resp{err: dummyError},
			},
		},
		{
			name: "two_errors",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{err: dummyError}, nodes[1]: resp{err: dummyError}, nodes[2]: resp{d: testData},
			},
			err: dummyError,
		},
		{
			name: "one_different",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{d: []byte("rest")}, nodes[1]: resp{d: testData}, nodes[2]: resp{d: testData},
			},
		},
		{
			name: "all_different_data",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{d: []byte("rest")}, nodes[1]: resp{d: []byte("fest")}, nodes[2]: resp{d: testData},
			},
			err: storage.ErrQuorumNotReached,
		},
		{
			name: "all_different_errors",
			resps: map[storage.ServiceAddr]resp{
				nodes[0]: resp{err: errors.New("err1")}, nodes[1]: resp{err: errors.New("err2")}, nodes[2]: resp{err: errors.New("err3")},
			},
			err: storage.ErrQuorumNotReached,
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			test := tests[i]
			nc := new(MockNode)
			nf := router.NewNodesFinder(FakeHasher{
				t: t,
				hashes: map[storage.ServiceAddr]uint64{
					nodes[0]: 1,
					nodes[1]: 2,
					nodes[2]: 3,
				},
			})
			fe := New(Config{
				RC:     &rc,
				NC:     nc,
				NF:     nf,
				Router: "router",
			})

			nc.get = get(t, nodes, key, func(node storage.ServiceAddr) ([]byte, error) {
				resp := test.resps[node]
				return resp.d, resp.err
			})
			got, err := fe.Get(key)
			if err != test.err {
				t.Fatalf("Get() error: %v, want %v", err, test.err)
			}
			if test.err != nil {
				return
			}
			if !reflect.DeepEqual(got, testData) {
				t.Errorf("Wrong data: got %s, want %s", got, testData)
			}
		})
	}
}

func TestGet_InitOnce(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("test")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	var cnt uint32
	rc.list = func(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
		atomic.AddUint32(&cnt, +1)
		return nodes, nil
	}

	nf := router.NewNodesFinder(FakeHasher{
		t: t,
		hashes: map[storage.ServiceAddr]uint64{
			nodes[0]: 1,
			nodes[1]: 2,
			nodes[2]: 3,
		},
	})
	nc := new(MockNode)
	nc.get = func(node storage.ServiceAddr, key storage.RecordID) ([]byte, error) {
		return testData, nil
	}

	fe := New(Config{
		RC:     &rc,
		NC:     nc,
		NF:     nf,
		Router: "router",
	})
	n := 5
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i%2) * 50 * time.Millisecond)
			got, err := fe.Get(key)
			if err != nil {
				t.Errorf("Get() error: %v", err)
			}
			if !reflect.DeepEqual(got, testData) {
				t.Errorf("Wrong data: got %s, want %s", got, testData)
			}
		}(i)
	}
	wg.Wait()
	if atomic.LoadUint32(&cnt) != 1 {
		t.Fatalf("List() was expected to be called one time exactly")
	}
}

func TestGet_Timing(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("test")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	rc.list = func(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
		return nodes, nil
	}

	sleep := 250 * time.Millisecond
	sleeps := map[storage.ServiceAddr]time.Duration{
		nodes[0]: sleep,
		nodes[1]: sleep * 2,
		nodes[2]: sleep * 3,
	}

	nc.get = get(t, nodes, key, func(node storage.ServiceAddr) ([]byte, error) {
		time.Sleep(sleeps[node])
		return testData, nil
	})

	nf := router.NewNodesFinder(FakeHasher{
		t: t,
		hashes: map[storage.ServiceAddr]uint64{
			nodes[0]: 1,
			nodes[1]: 2,
			nodes[2]: 3,
		},
	})
	fe := New(Config{
		RC:     &rc,
		NC:     &nc,
		NF:     nf,
		Router: "router",
	})

	start := time.Now()
	got, err := fe.Get(key)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !reflect.DeepEqual(got, testData) {
		t.Errorf("Wrong data: got %s, want %s", got, testData)
	}
	if diff := time.Since(start); !eqTime(diff, 2*sleep) {
		t.Errorf("Get() took too long")
	}
}

func TestParallelOps(t *testing.T) {
	key := storage.RecordID(1)
	testData := []byte("test")
	nodes := []storage.ServiceAddr{"node1", "node2", "node3"}

	nf := router.NewNodesFinder(FakeHasher{
		t: t,
		hashes: map[storage.ServiceAddr]uint64{
			nodes[0]: 1,
			nodes[1]: 2,
			nodes[2]: 3,
		},
	})

	rc.list = func(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
		return nodes, nil
	}
	rc.nodesFind = nodesFind(t, cfg, key, nodes, nil)

	nc := new(MockNode)
	nc.del = func(node storage.ServiceAddr, key storage.RecordID) error {
		return nil
	}
	nc.put = func(node storage.ServiceAddr, key storage.RecordID, data []byte) error {
		return nil
	}
	nc.get = func(node storage.ServiceAddr, key storage.RecordID) ([]byte, error) {
		return testData, nil
	}
	fe := New(Config{
		RC:     &rc,
		NC:     nc,
		NF:     nf,
		Router: "router",
	})

	go func() {
		for {
			runtime.Gosched()
			if err := fe.Del(key); err != nil {
				t.Errorf("Del() error: %v", err)
			}
		}
	}()
	go func() {
		for {
			runtime.Gosched()
			if err := fe.Put(key, testData); err != nil {
				t.Errorf("Put() error: %v", err)
			}
		}
	}()
	go func() {
		for {
			runtime.Gosched()
			got, err := fe.Get(key)
			if err != nil {
				t.Errorf("Get() error: %v", err)
			}
			if !reflect.DeepEqual(got, testData) {
				t.Errorf("Wrong data: got %s, want %s", got, testData)
			}
		}
	}()
	time.Sleep(3 * time.Second)
}
