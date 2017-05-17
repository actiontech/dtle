package store

import (
	"fmt"
	"log"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
)

const (
	backend  = "consul"
	keyspace = "udup"
)

type Store struct {
	logger *log.Logger
	Client store.Store
}

func init() {
	consul.Register()
}

func NewConsulStore(addrs []string, logger *log.Logger) (*Store, error) {
	c, err := libkv.NewStore(store.Backend(backend), addrs, nil)
	if err != nil {
		return nil, fmt.Errorf("consul store setup failed: %v", err)
	}

	_, err = c.List(keyspace)
	if err != store.ErrKeyNotFound && err != nil {
		return nil, fmt.Errorf("store backend not reachable: %v", err)
	}

	s := &Store{
		logger: logger,
		Client: c,
	}
	return s, nil
}

// Retrieve the leader from the store
func (s *Store) GetLeader() []byte {
	res, err := s.Client.Get(s.LeaderKey())
	if err != nil {
		s.logger.Printf("[ERR] store: Store not reachable: %v", err)
		return nil
	}

	return res.Value
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return keyspace + "/leader"
}
