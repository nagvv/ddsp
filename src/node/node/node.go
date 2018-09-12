package node

import (
	"sync"
	"time"

	router "router/client"
	"storage"
)

// Config stores configuration for a Node service.
//
// Config -- содержит конфигурацию Node.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Node.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr
	// Hearbeat is a time interval between hearbeats.
	// Hearbeat -- интервал между двумя heartbeats.
	Heartbeat time.Duration

	// Client specifies client for Router.
	// Client -- клиент для Router.
	Client router.Client `yaml:"-"`
}

// Node is a Node service.
type Node struct {
	sync.RWMutex
	cfg     Config
	hbch    chan int
	Storage map[storage.RecordID][]byte
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	return &Node{cfg: cfg, hbch: make(chan int), Storage: make(map[storage.RecordID][]byte)}
}

// Hearbeats runs heartbeats from node to a router
// each time interval set by cfg.Hearbeat.
//
// Hearbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	go func() {
		for {
			time.Sleep(node.cfg.Heartbeat)
			select {
			case <-node.hbch:
				return
			default:
				node.cfg.Client.Heartbeat(node.cfg.Router, node.cfg.Addr)
			}
		}
	}()
}

// Stop stops heartbeats
//
// Stop останавливает отправку heartbeats.
func (node *Node) Stop() {
	node.hbch <- 1
}

// Put an item to the node if an item for the given key doesn't exist.
// Returns the storage.ErrRecordExists error otherwise.
//
// Put -- добавить запись в node, если запись для данного ключа
// не существует. Иначе вернуть ошибку storage.ErrRecordExists.
func (node *Node) Put(k storage.RecordID, d []byte) error {
	node.Lock()
	defer node.Unlock()
	if _, ok := node.Storage[k]; ok {
		return storage.ErrRecordExists
	}
	node.Storage[k] = d
	return nil
}

// Del an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Del -- удалить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Del(k storage.RecordID) error {
	node.Lock()
	defer node.Unlock()
	if _, ok := node.Storage[k]; !ok {
		return storage.ErrRecordNotFound
	}
	delete(node.Storage, k)
	return nil
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	node.RLock()
	defer node.RUnlock()
	d, ok := node.Storage[k]
	if !ok {
		return nil, storage.ErrRecordNotFound
	}
	return d, nil
}
