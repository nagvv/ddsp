package node

import (
	"time"

	router "router/client"
	"storage"

	"sync"
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
	cfg     Config
	hbch    chan int
	Storage sync.Map
}

// New creates a new Node with a given cfg.
//
// New создает новый Node с данным cfg.
func New(cfg Config) *Node {
	return &Node{cfg: cfg, hbch: make(chan int)}
}

// Hearbeats runs heartbeats from node to a router
// each time interval set by cfg.Hearbeat.
//
// Hearbeats запускает отправку heartbeats от node к router
// через каждый интервал времени, заданный в cfg.Heartbeat.
func (node *Node) Heartbeats() {
	go func() { // FIXME: maybe use Ticker?
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
	if _, loaded := node.Storage.LoadOrStore(k, d); loaded {
		return storage.ErrRecordExists
	}
	return nil
}

// delLock makes Del() atomic
var delLock sync.Mutex

// Del an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Del -- удалить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Del(k storage.RecordID) error {
	delLock.Lock()
	defer delLock.Unlock()
	if _, ok := node.Storage.Load(k); ok {
		node.Storage.Delete(k)
		return nil
	}
	return storage.ErrRecordNotFound
}

// Get an item from the node if an item exists for the given key.
// Returns the storage.ErrRecordNotFound error otherwise.
//
// Get -- получить запись из node, если запись для данного ключа
// существует. Иначе вернуть ошибку storage.ErrRecordNotFound.
func (node *Node) Get(k storage.RecordID) ([]byte, error) {
	if b, ok := node.Storage.Load(k); ok {
		return b.([]byte), nil
	} else {
		return nil, storage.ErrRecordNotFound
	}
}
