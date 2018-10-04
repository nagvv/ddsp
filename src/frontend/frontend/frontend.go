package frontend

import (
	"bytes"
	rclient "router/client"
	"router/router"
	"storage"
	"sync"
	"time"
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
	// TODO: implement
	cfg Config
	list []storage.ServiceAddr
	sync.RWMutex
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	// TODO: implement
	return &Frontend{cfg: cfg}
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	// TODO: implement
	//NF.NodesFind(k, []nodes) возвращает целевые ноды из данных нодов, использует хеш
	//в NC нету nodesfind
	allnodes, e := fe.cfg.RC.NodesFind(fe.cfg.Router, k)//больше всего похоже на пункт 1, возвращает какие то ноды
	if e != nil {
		return e
	}
	if len(allnodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}
	//RC.NodesFind не имеет проверки на количество нодов + пункт 2 => нужно добавть NF.NodesFind
	//nodes := fe.cfg.NF.NodesFind(k, allnodes)//но NF.NodesFind падает, т.к. внутри него hasher = nil
	var et []error
	for _, node := range allnodes {
		e := fe.cfg.NC.Put(node, k, d)
		if e != nil {
			et = append(et, e)
			for i := 0; i < len(et)-1; i++ {
				for j := i + 1; j < len(et); j++ {
					if et[i] == et[j] {
						return et[i]
					}
				}
			}
		}
	}

	if len(et) > 0 {
		return storage.ErrQuorumNotReached
	}
	return nil
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	// TODO: implement
	//NF.NodesFind(k, []nodes) возвращает целевые ноды из данных нодов, использует хеш
	//в NC нету nodesfind
	allnodes, e := fe.cfg.RC.NodesFind(fe.cfg.Router, k)//больше всего похоже на пункт 1, возвращает какие то ноды
	if e != nil {
		return e
	}
	if len(allnodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}
	//RC.NodesFind не имеет проверки на количество нодов + пункт 2 => нужно добавть NF.NodesFind
	//nodes := fe.cfg.NF.NodesFind(k, allnodes)//но NF.NodesFind падает, т.к. внутри него hasher = nil
	var et []error
	for _, node := range allnodes {
	 	e := fe.cfg.NC.Del(node, k)
		if e!=nil {
			et = append(et, e)
			for i := 0; i < len(et)-1; i++ {
				for j := i + 1; j < len(et); j++ {
					if et[i] == et[j] {
						return et[i]
					}
				}
			}
		}
	}
	if len(et) > 0 {
		return storage.ErrQuorumNotReached
	}
	return nil
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	// TODO: implement
	var e error
	fe.Lock()
	if len(fe.list) == 0 {
		for {
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
	for _, node := range nodes {
		td, e := fe.cfg.NC.Get(node, k)
		if e==nil {
			dt = append(dt, td)
			for i := 0; i < len(dt)-1; i++ {
				for j := i + 1; j < len(dt); j++ {
					if bytes.Equal(dt[i], dt[j]) {
						return dt[i], nil
					}
				}
			}
		} else {
			et = append(et, e)
			for i := 0; i < len(et)-1; i++ {
				for j := i + 1; j < len(et); j++ {
					if et[i]==et[j] {
						return nil, et[i]
					}
				}
			}
		}
	}
	return nil, storage.ErrQuorumNotReached
}
