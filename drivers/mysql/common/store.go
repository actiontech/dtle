package common

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/mysql"
	"strconv"
	"strings"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/consul"
)

func init() {
	consul.Register()
}

type StoreManager struct {
	consulStore store.Store
	logger      hclog.Logger
}
func NewStoreManager(consulAddr []string, logger hclog.Logger) (*StoreManager, error) {
	consulStore, err := libkv.NewStore(store.CONSUL, consulAddr, nil)
	if err != nil {
		return nil, err
	}
	return &StoreManager{
		consulStore: consulStore,
		logger:      logger,
	}, nil
}
func (sm *StoreManager) DestroyJob(jobName string) error {
	key := fmt.Sprintf("dtle/%v", jobName)
	err := sm.consulStore.DeleteTree(key)
	if err == store.ErrKeyNotFound {
		return nil
	} else {
		return err
	}
}
func (sm *StoreManager) SaveGtidForJob(jobName string, gtid string) error {
	key := fmt.Sprintf("dtle/%v/Gtid", jobName)
	err := sm.consulStore.Put(key, []byte(gtid), nil)
	return err
}
const binlogFilePosSeparator = "//dtle//"
func (sm *StoreManager) SaveBinlogFilePosForJob(jobName string, file string, pos int) error {
	key := fmt.Sprintf("dtle/%v/BinlogFilePos", jobName)
	s := fmt.Sprintf("%v%v%v", file, binlogFilePosSeparator, pos)
	err := sm.consulStore.Put(key, []byte(s), nil)
	return err
}
func (sm *StoreManager) GetBinlogFilePosForJob(jobName string) (*mysql.Position, error) {
	key := fmt.Sprintf("dtle/%v/BinlogFilePos", jobName)
	p, err := sm.consulStore.Get(key)
	if err == store.ErrKeyNotFound {
		return &mysql.Position{
			Name: "",
			Pos:  0,
		}, nil
	} else if err != nil {
		return nil, err
	}
	s := string(p.Value)
	ss := strings.Split(s, binlogFilePosSeparator)
	if len(ss) != 2 {
		return nil, fmt.Errorf("Unexpected BinlogFilePos format. value %v", s)
	}
	pos, err := strconv.Atoi(ss[1])
	if err != nil {
		return nil, errors.Wrap(err, "Atoi")
	}
	return &mysql.Position{
		Name: ss[0],
		Pos:  uint32(pos),
	}, nil
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

func (sm *StoreManager) PutAndWatchNats(jobName string, natsAddr string, stopCh chan struct{}, onErrorF func(error)) {
	sm.logger.Info("PutAndWatchNats")

	var err error
	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	err = sm.consulStore.Put(natsKey, []byte("waitdst"), nil)
	if err != nil {
		onErrorF(err)
		return
	}

	natsCh, err := sm.consulStore.Watch(natsKey, stopCh)
	if err != nil {
		onErrorF(err)
		return
	}

	loop := true
	for loop {
		select {
		case <-stopCh:
			return
		case kv := <-natsCh:
			if kv == nil {
				onErrorF(errors.Wrap(ErrNoConsul, "PutAndWatchNats"))
				return
			}
			s := string(kv.Value)
			sm.logger.Debug("NatsAddr. got", "value", s)
			if s == "waitdst" {
				// keep watching
			} else if s == "wait" {
				err = sm.consulStore.Put(natsKey, []byte(natsAddr), nil)
				if err != nil {
					onErrorF(err)
					return
				}
				loop = false
			} else {
				onErrorF(fmt.Errorf("PutAndWatchNats. unexpected value %v", s))
				return
			}
		}
	}

	go func() {
		for {
			select {
			case <-stopCh:
				return
			case kv := <-natsCh:
				if kv == nil {
					onErrorF(errors.Wrap(ErrNoConsul, "PutAndWatchNats"))
					return
				}
				s := string(kv.Value)
				if s == "wait" {
					onErrorF(fmt.Errorf("NatsAddr changed to %v. will restart dst", s))
					return
				}
			}
		}
	}()
}

func (sm *StoreManager) WatchNats(jobName string, stopCh chan struct{}) (<-chan *store.KVPair, error) {
	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	natsCh, err := sm.consulStore.Watch(natsKey, stopCh)
	if err != nil {
		return nil, err
	}
	return natsCh, nil
}

func (sm *StoreManager) PutNatsWait(jobName string) error {
	sm.logger.Debug("consul PutNatsWait")
	key := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	return sm.consulStore.Put(key, []byte("wait"), nil)
}

func (sm *StoreManager) PutKey(subject string, key string, value []byte) error {
	url := fmt.Sprintf("dtle/%v/%v", subject, key)
	return sm.consulStore.Put(url, value, nil)
}

func (sm *StoreManager) WaitKv(subject string, key string, stopCh chan struct{}) ([]byte, error) {
	url := fmt.Sprintf("dtle/%v/%v", subject, key)
	ch, err := sm.consulStore.Watch(url, stopCh)
	if err != nil {
		return nil, err
	}
	kv := <-ch
	if kv == nil {
		return nil, errors.Wrap(ErrNoConsul, "WaitKv")
	} else {
		return kv.Value, nil
	}
}

func GetGtidFromConsul(sm *StoreManager, subject string, logger hclog.Logger, mysqlContext *MySQLDriverConfig) error {
	gtid, err := sm.GetGtidForJob(subject)
	if err != nil {
		return errors.Wrap(err, "GetGtidForJob")
	}
	logger.Info("Got gtid from consul", "gtid", gtid)
	if gtid != "" {
		logger.Info("Use gtid from consul", "gtid", gtid)
		mysqlContext.Gtid = gtid
	}
	pos, err := sm.GetBinlogFilePosForJob(subject)
	if err != nil {
		return errors.Wrap(err, "GetBinlogFilePosForJob")
	}
	logger.Info("Got BinlogFile/Pos from consul",
		"file", mysqlContext.BinlogFile, "pos", mysqlContext.BinlogPos)
	if pos.Name != "" {
		mysqlContext.BinlogFile = pos.Name
		mysqlContext.BinlogPos = int64(pos.Pos)
		logger.Info("Use BinlogFile/Pos from consul",
			"file", mysqlContext.BinlogFile, "pos", mysqlContext.BinlogPos)
	}
	return nil
}

