package frontend

import (
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
	// TODO: implement
	cfg Config
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
	storages, e := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if e != nil {
		return e
	}
	nodes := fe.cfg.NF.NodesFind(k, storages)
	for _, node := range nodes {
		fe.cfg.NC.Put(node, k, d)
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
	storages, e := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if e != nil {
		return e
	}
	nodes := fe.cfg.NF.NodesFind(k, storages)
	for _, node := range nodes {
		fe.cfg.NC.Del(node, k)
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
	var storages []storage.ServiceAddr
	var e error
	for {
		storages, e = fe.cfg.RC.List(fe.cfg.Router)
		if e == nil {
			break
		}
		time.Sleep(InitTimeout)
	}
	nodes := fe.cfg.NF.NodesFind(k, storages)
	var d []byte
	for _, node := range nodes {
		d, e = fe.cfg.NC.Get(node, k)
	}
	return d, nil
}
