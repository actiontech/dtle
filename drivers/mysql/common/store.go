package common

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/mysql"

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
func (sm *StoreManager) DestroyJob(jobId string) error {
	key := fmt.Sprintf("dtle/%v", jobId)
	err := sm.consulStore.DeleteTree(key)
	if nil != err && store.ErrKeyNotFound != err {
		return err
	}
	key = fmt.Sprintf("dtleJobList/%v", jobId)
	err = sm.consulStore.DeleteTree(key)
	if nil != err && store.ErrKeyNotFound != err {
		return err
	}
	return nil
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
func (sm *StoreManager) GetJobInfo(jobId string) (*JobListItemV2, error) {
	key := fmt.Sprintf("dtleJobList/%v", jobId)
	kp, err := sm.consulStore.Get(key)
	if err == store.ErrKeyNotFound {
		return &JobListItemV2{}, nil
	}
	if nil != err {
		return nil, fmt.Errorf("get %v value from consul failed: %v", key, err)
	}
	job := new(JobListItemV2)
	err = json.Unmarshal(kp.Value, job)
	if err != nil {
		return nil, fmt.Errorf("get %v from consul, unmarshal err : %v", key, err)
	}

	return job, nil
}

func (sm *StoreManager) GetJobStatus(jobId string) (string, error) {
	jobInfo, err := sm.GetJobInfo(jobId)
	if err == store.ErrKeyNotFound {
		return "", nil
	} else if err != nil {
		return "", err
	}
	return jobInfo.JobStatus, nil
}

func (sm *StoreManager) FindJobList() ([]*JobListItemV2, error) {
	key := "dtleJobList/"
	kps, err := sm.consulStore.List(key)
	if nil != err && err != store.ErrKeyNotFound {
		return nil, fmt.Errorf("get %v value from consul failed: %v", key, err)
	}
	jobList := make([]*JobListItemV2, 0)

	for _, kp := range kps {
		job := new(JobListItemV2)
		err = json.Unmarshal(kp.Value, job)
		if err != nil {
			return nil, fmt.Errorf("get %v from consul, unmarshal err : %v", key, err)
		}
		jobList = append(jobList, job)
	}
	return jobList, nil
}

func (sm *StoreManager) SaveJobInfo(job JobListItemV2) error {
	key := fmt.Sprintf("dtleJobList/%v", job.JobId)
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("save %v to consul, marshal err : %v", key, err)
	}
	err = sm.consulStore.Put(key, jobBytes, nil)
	return err
}

func (sm *StoreManager) WaitOnJob(currentJob string, waitJob string, stopCh chan struct{}) error {
	key1 := fmt.Sprintf("dtleJobList/%v", waitJob)
	// NB: it is OK to watch on non-existing keys.
	ch, err := sm.consulStore.Watch(key1, stopCh)
	if err != nil {
		return err
	}
	for {
		kv := <-ch
		if kv == nil {
			return fmt.Errorf("WaitOnJob get nil kv. current task might have been shutdown")
		}
		job := new(JobListItemV2)
		err = json.Unmarshal(kv.Value, job)
		if err != nil {
			return fmt.Errorf("watch %v from consul, unmarshal err : %v", key1, err)
		}
		if job.JobStatus == TargetGtidFinished {
			break
		}
	}

	// update reverse job status
	currentJobInfo, err := sm.GetJobInfo(currentJob)
	if err != nil {
		sm.logger.Error("job_id=%v; get job info failed: %v", currentJob, err)
		return err
	} else {
		currentJobInfo.JobId = currentJob
		currentJobInfo.JobStatus = DtleJobStatusNonPaused
		err = sm.SaveJobInfo(*currentJobInfo)
		if err != nil {
			sm.logger.Error("job_id=%v; save job info failed: %v", currentJob, err)
			return err
		}
	}

	return nil
}

func (sm *StoreManager) PutTargetGtid(subject string, value string) error {
	return sm.PutKey(subject, "targetGtid", []byte(value))
}

func (sm *StoreManager) WatchTargetGtid(subject string, stopCh chan struct{}) (string, error) {
	key := fmt.Sprintf("dtle/%v/targetGtid", subject)
	ch, err := sm.consulStore.Watch(key, stopCh)
	if err != nil {
		return "", err
	}

	kv := <-ch
	if kv == nil {
		return "", fmt.Errorf("WatchTargetGtid. got nil kv. might have been shutdown")
	}

	return string(kv.Value), nil
}

func (sm *StoreManager) GetTargetGtid(subject string) (string, error) {
	key := fmt.Sprintf("dtle/%v/targetGtid", subject)
	kv, err := sm.consulStore.Get(key)
	if err == store.ErrKeyNotFound {
		return "", nil
	} else if err != nil {
		return "", err
	}

	return string(kv.Value), nil
}

func (sm *StoreManager) FindUserList(userKey string) ([]*User, error) {
	userKey = fmt.Sprintf("%s/%s", "dtleUser", userKey)
	storeUsers, err := sm.consulStore.List(userKey)
	if nil != err && err != store.ErrKeyNotFound {
		return nil, fmt.Errorf("get %v value from consul failed: %v", userKey, err)
	}
	userList := make([]*User, 0)
	for _, storeUser := range storeUsers {
		user := new(User)
		err = json.Unmarshal(storeUser.Value, user)
		if err != nil {
			return nil, fmt.Errorf("get %v from consul, unmarshal err : %v", storeUser, err)
		}
		user.Password = "*"
		userList = append(userList, user)
	}

	return userList, nil
}

func (sm *StoreManager) FindTenantList() (tenants []string, err error) {
	tenantKey := "dtleUser"
	storeUsers, err := sm.consulStore.List(tenantKey)
	if nil != err && err != store.ErrKeyNotFound {
		return tenants, fmt.Errorf("get %v value from consul failed: %v", tenantKey, err)
	}
	tenantMap := make(map[string]struct{}, 0)
	for _, storeUser := range storeUsers {
		user := new(User)
		err = json.Unmarshal(storeUser.Value, user)
		if err != nil {
			return tenants, fmt.Errorf("get %v from consul, unmarshal err : %v", storeUser, err)
		}
		tenantMap[user.Tenant] = struct{}{}
	}
	for tenant, _ := range tenantMap {
		tenants = append(tenants, tenant)
	}
	return
}

func (sm *StoreManager) DeleteUser(tenant, user string) error {
	key := fmt.Sprintf("dtleUser/%v/%v", tenant, user)
	err := sm.consulStore.Delete(key)
	if nil != err && store.ErrKeyNotFound != err {
		return err
	}
	return nil
}

var once sync.Once

func (sm *StoreManager) GetUser(tenant, username string) (*User, bool, error) {
	key := fmt.Sprintf("dtleUser/%v/%v", tenant, username)
	exists, err := sm.consulStore.Exists(key)
	if err != nil {
		return nil, exists, err
	} else if !exists {
		return nil, exists, nil
	}

	kp, err := sm.consulStore.Get(key)
	if nil != err {
		return nil, false, err
	}
	user := new(User)
	err = json.Unmarshal(kp.Value, user)
	if err != nil {
		return nil, false, fmt.Errorf("get %v from consul, unmarshal err : %v", key, err)
	}
	return user, true, nil
}
func (sm *StoreManager) SaveUser(user *User) error {
	key := fmt.Sprintf("dtleUser/%v/%v", user.Tenant, user.Username)
	jobBytes, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("save %v to consul, marshal err : %v", key, err)
	}
	err = sm.consulStore.Put(key, jobBytes, nil)
	return err
}

func (sm *StoreManager) FindRoleList(tenant string) ([]*Role, error) {
	key := fmt.Sprintf("%s/%s", "dtleRole", tenant)
	storeUsers, err := sm.consulStore.List(key)
	if nil != err && err != store.ErrKeyNotFound {
		return nil, fmt.Errorf("get %v value from consul failed: %v", key, err)
	}
	roleList := make([]*Role, 0)
	for _, storeUser := range storeUsers {
		role := new(Role)
		err = json.Unmarshal(storeUser.Value, role)
		if err != nil {
			return nil, fmt.Errorf("get %v from consul, unmarshal err : %v", storeUser, err)
		}
		roleList = append(roleList, role)
	}

	return roleList, nil
}

func (sm *StoreManager) GetRole(tenant, name string) (*Role, bool, error) {
	key := fmt.Sprintf("dtleRole/%s/%s", tenant, name)
	exists, err := sm.consulStore.Exists(key)
	if err != nil {
		return nil, exists, err
	} else if !exists {
		return nil, exists, nil
	}

	kp, err := sm.consulStore.Get(key)
	if nil != err {
		return nil, false, err
	}
	role := new(Role)
	err = json.Unmarshal(kp.Value, role)
	if err != nil {
		return nil, false, fmt.Errorf("get %v from consul, unmarshal err : %v", key, err)
	}
	return role, true, nil
}

func (sm *StoreManager) SaveRole(role *Role) error {
	key := fmt.Sprintf("dtleRole/%v/%v", role.Tenant, role.Name)
	jobBytes, err := json.Marshal(role)
	if err != nil {
		return fmt.Errorf("save %v to consul, marshal err : %v", key, err)
	}
	err = sm.consulStore.Put(key, jobBytes, nil)
	return err
}

func (sm *StoreManager) DeleteRole(tenant, name string) error {
	key := fmt.Sprintf("dtleRole/%v/%v", tenant, name)
	err := sm.consulStore.Delete(key)
	if nil != err && store.ErrKeyNotFound != err {
		return err
	}
	return nil
}

// consul store item

func NewDefaultRole(tenant string) *Role {
	return &Role{
		Tenant:      tenant,
		Name:        DefaultRole,
		ObjectUsers: nil,
		ObjectType:  "all",
		Authority:   DefaultAdminAuth,
	}
}

type Role struct {
	Tenant      string   `json:"tenant"`
	Name        string   `json:"name"`
	ObjectUsers []string `json:"object_users"`
	ObjectType  string   `json:"object_type"`
	Authority   string   `json:"authority"`
}

type User struct {
	Username   string `json:"username"`
	Tenant     string `json:"tenant"`
	Role       string `json:"role"`
	Password   string `json:"password"`
	CreateTime string `json:"create_time"`
	Remark     string `json:"remark"`
}

type JobListItemV2 struct {
	JobId         string    `json:"job_id"`
	JobStatus     string    `json:"job_status"`
	Topic         string    `json:"topic"`
	JobCreateTime string    `json:"job_create_time"`
	SrcAddrList   []string  `json:"src_addr_list"`
	DstAddrList   []string  `json:"dst_addr_list"`
	User          string    `json:"user"`
	JobSteps      []JobStep `json:"job_steps"`
}

type JobStep struct {
	StepName      string  `json:"step_name"`
	StepStatus    string  `json:"step_status"`
	StepSchedule  float64 `json:"step_schedule"`
	JobCreateTime string  `json:"job_create_time"`
}

func NewJobStep(stepName string) JobStep {
	return JobStep{
		StepName:      stepName,
		StepStatus:    "start",
		StepSchedule:  0,
		JobCreateTime: time.Now().In(time.Local).Format(time.RFC3339),
	}
}
