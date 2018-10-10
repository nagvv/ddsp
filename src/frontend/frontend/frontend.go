package frontend

import (
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
	once sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{cfg: cfg}
}

func (fe *Frontend) putDel(k storage.RecordID, job func(node storage.ServiceAddr) error) error {
	nodes, err := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if err != nil {
		return err
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
		err := <-ch
		if err != nil {
			et[err]++
		}
	}

	for err, n := range et {
		if n >= storage.MinRedundancy {
			return err
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
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.cfg.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.once.Do(func() {		
		for {
			var err error
			fe.list, err = fe.cfg.RC.List(fe.cfg.Router)
			if err == nil {
				break
			}
			time.Sleep(InitTimeout)
		}
	})

	nodes := fe.cfg.NF.NodesFind(k, fe.list)
	dataMap := make(map[string]int)
	errorMap := make(map[error]int)

	type result struct {
		data []byte
		err error
	}

	resChan := make(chan result, len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			tempData, tempError := fe.cfg.NC.Get(node, k)
			resChan <- result{tempData, tempError}
		}(node)
	}

	for range nodes {
		result := <-resChan
		err := result.err
		data := result.data
		if err == nil {
			dataMap[string(data)]++
			if dataMap[string(data)] >= storage.MinRedundancy {
				return data, nil
			}
			continue
		}
		errorMap[err]++
		if errorMap[err] >= storage.MinRedundancy {
			return nil, err
		}
	}
	return nil, storage.ErrQuorumNotReached
}
