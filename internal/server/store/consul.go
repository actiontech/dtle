package store

import (
	"log"
	"fmt"

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

func NewConsulStore(addrs []string) (*Store, error) {
	c, err := libkv.NewStore(store.Backend(backend), addrs, nil)
	if err != nil {
		return nil, fmt.Errorf("consul store setup failed: %v", err)
	}

	_, err = c.List(keyspace)
	if err != store.ErrKeyNotFound && err != nil {
		return nil, fmt.Errorf("store backend not reachable: %v", err)
	}

	s := &Store{
		Client:        c,
	}
	return s,nil
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return keyspace + "/leader"
}

