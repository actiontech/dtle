/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package store

import (
	"fmt"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"

	ulog "udup/internal/logger"
)

const (
	backend  = "consul"
	keyspace = "udup"
)

type Store struct {
	logger *ulog.Logger
	Client store.Store
}

func init() {
	consul.Register()
}

func NewConsulStore(addrs []string, logger *ulog.Logger) (*Store, error) {
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
		s.logger.Errorf("store: Store not reachable: %v", err)
		return nil
	}

	return res.Value
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return keyspace + "/leader"
}
