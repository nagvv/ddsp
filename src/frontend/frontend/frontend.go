package frontend

import (
	"bytes"
	"sync"
	"time"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	cfg  Config
	list []storage.ServiceAddr
	sync.Mutex
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{cfg: cfg}
}

func (fe *Frontend) pudel(k storage.RecordID, job func(node storage.ServiceAddr) error) error {
	nodes, e := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if e != nil {
		return e
	}
	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	et := make(map[error]int)
	ch := make(chan error, len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			ch <- job(node)
		}(node)
	}

	for range nodes {
		e := <-ch
		if e != nil {
			et[e]++
		}
	}

	for e, n := range et {
		if n >= storage.MinRedundancy {
			return e
		}
	}

	if len(nodes)-len(et) >= storage.MinRedundancy {
		return nil
	}

	return storage.ErrQuorumNotReached
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.pudel(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.pudel(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.Lock()
	if len(fe.list) == 0 {
		for {
			var e error
			fe.list, e = fe.cfg.RC.List(fe.cfg.Router)
			if e == nil {
				break
			}
			time.Sleep(InitTimeout)
		}
	}
	fe.Unlock()

	nodes := fe.cfg.NF.NodesFind(k, fe.list)
	var dt [][]byte
	var et []error
	che := make(chan error, len(nodes))
	chd := make(chan []byte, len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			td, te := fe.cfg.NC.Get(node, k)
			chd <- td
			che <- te
		}(node)
	}

	for range nodes {
		td := <-chd
		e := <-che
		if e == nil {
			dt = append(dt, td)
			for i := 0; i < len(dt)-1; i++ {
				for j := i + 1; j < len(dt); j++ {
					if bytes.Equal(dt[i], dt[j]) {
						return dt[i], nil
					}
				}
			}
			continue
		} else {
			et = append(et, e)
			for i := 0; i < len(et)-1; i++ {
				for j := i + 1; j < len(et); j++ {
					if et[i] == et[j] {
						return nil, et[i]
					}
				}
			}
			continue
		}
	}
	return nil, storage.ErrQuorumNotReached
}
