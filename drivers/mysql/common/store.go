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

func (sm *StoreManager) DstPutNats(jobName string, natsAddr string, stopCh chan struct{}, onWatchError func(error)) error {
	sm.logger.Debug("DstPutNats")

	var err error
	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	err = sm.consulStore.Put(natsKey, []byte("waitdst"), nil)
	if err != nil {
		return err
	}

	natsCh, err := sm.consulStore.Watch(natsKey, stopCh)
	if err != nil {
		return err
	}

	loop := true
	for loop {
		select {
		case <-stopCh:
			return err
		case kv := <-natsCh:
			if kv == nil {
				return errors.Wrap(ErrNoConsul, "DstPutNats")
			}
			s := string(kv.Value)
			sm.logger.Info("NatsAddr. got", "value", s)
			if s == "waitdst" {
				// keep watching
			} else if s == "wait" {
				err = sm.consulStore.Put(natsKey, []byte(natsAddr), nil)
				if err != nil {
					return err
				}
				loop = false
			} else {
				return fmt.Errorf("DstPutNats. unexpected value %v", s)
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
					onWatchError(errors.Wrap(ErrNoConsul, "DstPutNats"))
					return
				}
				s := string(kv.Value)
				sm.logger.Info("NatsAddr. got after put addr", "value", s)
				if s == "wait" {
					onWatchError(fmt.Errorf("NatsAddr changed to %v. will restart dst", s))
					return
				}
			}
		}
	}()

	return nil
}

func (sm *StoreManager) SrcWatchNats(jobName string, stopCh chan struct{},
	onWatchError func(error)) (natsAddr string, err error) {

	sm.logger.Debug("SrcWatchNats")

	natsKey := fmt.Sprintf("dtle/%v/NatsAddr", jobName)
	natsCh, err := sm.consulStore.Watch(natsKey, stopCh)
	if err != nil {
		return "", err
	}

	hasPutWait := false
	for natsAddr == "" {
		select {
		case <-stopCh:
			sm.logger.Info("shutdown when watching NatsAddr")
			return
		case kv := <-natsCh:
			if kv == nil {
				return "", errors.Wrap(ErrNoConsul, "SrcWatchNats")
			}
			s := string(kv.Value)
			if s == "waitdst" {
				sm.logger.Info("NatsAddr. got waitdst. will put wait")
				err = sm.consulStore.Put(natsKey, []byte("wait"), nil)
				if err != nil {
					return "", errors.Wrap(err, "PutNatsWait")
				}
			} else if s == "wait" {
				// Put by this round or previous round of src.
				sm.logger.Info("NatsAddr. got wait")
				hasPutWait = true
			} else {
				// an addr
				if hasPutWait {
					natsAddr = s
					sm.logger.Info("NatsAddr. got addr", "addr", s)
				} else {
					// Got an addr before src asks for it.
					// An addr of previous round.
					// Put wait to trigger dst restart.
					sm.logger.Info("NatsAddr. got addr before having put wait", "addr", s)
					err = sm.consulStore.Put(natsKey, []byte("wait"), nil)
					if err != nil {
						return "", errors.Wrap(err, "PutNatsWait")
					}
				}
			}
		}
	}

	go func() {
		select {
		case <-stopCh:
			return
		case kv := <-natsCh:
			if kv == nil {
				onWatchError(errors.Wrap(ErrNoConsul, "SrcWatchNats"))
				return
			}
			s := string(kv.Value)
			onWatchError(fmt.Errorf("NatsAddr changed to %v. will restart src", s))
		}
	}()

	return natsAddr, nil
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

func (sm *StoreManager) PutJobPauseStatus(jobName string, isPaused bool) error {
	url := fmt.Sprintf("dtle/JobStatus/%v/IsPaused", jobName)
	return sm.consulStore.Put(url, []byte(strconv.FormatBool(isPaused)), nil)
}

func (sm *StoreManager) GetJobPauseStatusIfExist(jobName string) (isExisted, isPaused bool, err error) {
	key := fmt.Sprintf("dtle/JobStatus/%v/IsPaused", jobName)
	isExisted, err = sm.consulStore.Exists(key)
	if nil != err {
		return false, false, fmt.Errorf("verify key %v from consul failed: %v", key, err)
	}

	if !isExisted {
		return false, false, nil
	}

	kp, err := sm.consulStore.Get(key)
	if nil != err {
		return false, false, fmt.Errorf("get %v from consul failed: %v", key, err)
	}
	isPaused, err = strconv.ParseBool(string(kp.Value))
	if nil != err {
		return false, false, fmt.Errorf("parse value of key %v from consul failed: %v", key, err)
	}
	return true, isPaused, nil
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

func GetPausedStatusFromConsul(storeManager *StoreManager, subject string) (bool, error) {
	_, isPaused, err := storeManager.GetJobPauseStatusIfExist(subject)
	if nil != err {
		return false, err
	}

	return isPaused, nil
}
