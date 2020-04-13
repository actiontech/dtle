package common

import (
	"fmt"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
)

func init() {
	consul.Register()
}

type StoreManager struct {
	consulStore store.Store
}
func NewStoreManager(consulAddr []string) (*StoreManager, error) {
	consulStore, err := libkv.NewStore(store.CONSUL, consulAddr, nil)
	if err != nil {
		return nil, err
	}
	return &StoreManager{consulStore:consulStore}, nil
}
func (sm *StoreManager) SaveGtidForJob(jobName string, gtid string) error {
	key := fmt.Sprintf("dtle/%v/Gtid", jobName)
	err := sm.consulStore.Put(key, []byte(gtid), nil)
	return err
}
func (sm *StoreManager) GetGtidForJob(jobName string) (string, error) {
	key := fmt.Sprintf("dtle/%v/Gtid", jobName)
	p, err := sm.consulStore.Get(key)
	if err == store.ErrKeyNotFound {
		return "", nil
	} else if err != nil {
		return "", err
	}
	// Get a non-existing KV
	return string(p.Value), nil
}

func (sm *StoreManager) PutNats(jobName string, natsAddr string) error {
	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	return sm.consulStore.Put(natsKey, []byte(natsAddr), nil)
}

func (sm *StoreManager) WatchNats(jobName string, stopCh chan struct{}) (string, error) {
	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	natsCh, err := sm.consulStore.Watch(natsKey, stopCh)
	if err != nil {
		return "", err
	}
	kv := <-natsCh
	return string(kv.Value), nil
}
