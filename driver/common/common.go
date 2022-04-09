package common

import (
	"bytes"
	compress "compress/gzip"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/pingcap/tidb/types"

	"github.com/actiontech/dtle/g"
	"github.com/go-mysql-org/go-mysql/mysql"
	uuid "github.com/satori/go.uuid"
)

const (
	DefaultConnectWaitSecond = 10
	DefaultConnectWait       = DefaultConnectWaitSecond * time.Second

	DtleJobStatusNonPaused   = "non-paused"
	DtleJobStatusPaused      = "paused"
	DtleJobStatusUndefined   = "undefined"
	DtleJobStatusReverseInit = "reverse-init"
	DtleJobStatusStop        = "stop"
	TargetGtidFinished       = "finished"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

const (
	DtleFlagCreateSchemaIfNotExists = 0x1
)

const (
	// TODO: Using configuration to set jwt secret
	JWTSecret              = "secret"
	DefaultAdminTenant     = "platform"
	DefaultAdminUser       = "admin"
	DefaultAdminPwd        = "admin"
	DefaultEncryptAdminPwd = "rAMd28u1e8j2cGfXeuef6ChGe8+SFP0b29KuJ0w9hvo2B2HbVTYaAp2E+vdBBE3KpKfo1HfOOezy8WFdk40v/xjM6Gwfhmji2czpKdfaGdnbkvChfqv2taPo8WFeRaGiaIEZ1Ygu0eKz1Yq+FOIEwNjH+clthxPqX3hiizSBbHA="
	DefaultRole            = "admin"
	DefaultAdminAuth       = "[{\"name\":\"service\",\"text_cn\":\"服务\",\"text_en\":\"service\",\"menu_level\":1,\"menu_url\":\"\",\"id\":1,\"parent_id\":0,\"operations\":[]},{\"name\":\"migration\",\"text_cn\":\"数据迁移\",\"text_en\":\"migration\",\"menu_level\":2,\"menu_url\":\"/migration\",\"id\":2,\"parent_id\":1,\"operations\":[{\"action\":\"migration-list\",\"uri\":\"/v2/jobs/migration\",\"text_cn\":\"迁移任务列表\",\"text_en\":\"migration job list\"},{\"action\":\"migration-create\",\"uri\":\"/v2/job/migration/create\",\"text_cn\":\"迁移创建任务\",\"text_en\":\"create migration job\"},{\"action\":\"migration-pause\",\"uri\":\"/v2/job/migration/pause\",\"text_cn\":\"暂停任务\",\"text_en\":\"pause migration job\"},{\"action\":\"migration-resume\",\"uri\":\"/v2/job/migration/resume\",\"text_cn\":\"重启迁移任务\",\"text_en\":\"resume migration job\"},{\"action\":\"migration-delete\",\"uri\":\"/v2/job/migration/delete\",\"text_cn\":\"销毁迁移任务\",\"text_en\":\"delete migration job\"},{\"action\":\"migration-reverse\",\"uri\":\"/v2/job/migration/reverse\",\"text_cn\":\"创建迁移反向复制任务\",\"text_en\":\"reverse migration job\"},{\"action\":\"migration-reverse_start\",\"uri\":\"/v2/job/migration/reverse_start\",\"text_cn\":\"启动迁移反向任务\",\"text_en\":\"start migration reverse job \"},{\"action\":\"migration-update\",\"uri\":\"/v2/job/migration/update\",\"text_cn\":\"修改迁移任务\",\"text_en\":\"update migration job\"},{\"action\":\"migration-detail\",\"uri\":\"/v2/job/migration/detail\",\"text_cn\":\"查看迁移任务详情\",\"text_en\":\"migration detail job\"}]},{\"name\":\"sync\",\"text_cn\":\"数据同步\",\"text_en\":\"sync\",\"menu_level\":2,\"menu_url\":\"/sync\",\"id\":3,\"parent_id\":1,\"operations\":[{\"action\":\"sync-list\",\"uri\":\"/v2/jobs/sync\",\"text_cn\":\"同步任务列表\",\"text_en\":\"sync job list\"},{\"action\":\"sync-create\",\"uri\":\"/v2/job/sync/create\",\"text_cn\":\"创建同步任务\",\"text_en\":\"create sync job\"},{\"action\":\"sync-pause\",\"uri\":\"/v2/job/sync/pause\",\"text_cn\":\"暂停同步任务\",\"text_en\":\"pause sync job\"},{\"action\":\"sync-resume\",\"uri\":\"/v2/job/sync/resume\",\"text_cn\":\"重启同步任务\",\"text_en\":\"resume sync job\"},{\"action\":\"sync-delete\",\"uri\":\"/v2/job/sync/delete\",\"text_cn\":\"销毁同步任务\",\"text_en\":\"delete sync job\"},{\"action\":\"sync-reverse\",\"uri\":\"/v2/job/sync/reverse\",\"text_cn\":\"反向复制同步任务\",\"text_en\":\"reverse sync job\"},{\"action\":\"sync-reverse_start\",\"uri\":\"/v2/job/sync/reverse_start\",\"text_cn\":\"启动同步反向任务\",\"text_en\":\"reverse start sync job\"},{\"action\":\"sync-update\",\"uri\":\"/v2/job/sync/update\",\"text_cn\":\"修改同步任务\",\"text_en\":\"update sync job\"},{\"action\":\"sync-detail\",\"uri\":\"/v2/job/sync/detail\",\"text_cn\":\"查看同步任务详情\",\"text_en\":\"sync job detail \"}]},{\"name\":\"subscription\",\"text_cn\":\"数据订阅\",\"text_en\":\"subscription\",\"menu_level\":2,\"menu_url\":\"/subscribe\",\"id\":4,\"parent_id\":1,\"operations\":[{\"action\":\"subscription-list\",\"uri\":\"/v2/jobs/subscription\",\"text_cn\":\"订阅任务列表\",\"text_en\":\"subscription job list\"},{\"action\":\"subscription-create\",\"uri\":\"/v2/job/subscription/create\",\"text_cn\":\"创建订阅任务\",\"text_en\":\"create subscription job\"},{\"action\":\"subscription-pause\",\"uri\":\"/v2/job/subscription/pause\",\"text_cn\":\"暂停订阅任务\",\"text_en\":\"pause subscription job\"},{\"action\":\"subscription-resume\",\"uri\":\"/v2/job/subscription/resume\",\"text_cn\":\"重启订阅任务\",\"text_en\":\"resume subscription job\"},{\"action\":\"subscription-delete\",\"uri\":\"/v2/job/subscription/delete\",\"text_cn\":\"销毁订阅任务\",\"text_en\":\"delete subscription job\"},{\"action\":\"subscription-update\",\"uri\":\"/v2/job/subscription/update\",\"text_cn\":\"修改订阅任务\",\"text_en\":\"update subscription job\"},{\"action\":\"subscription-detail\",\"uri\":\"/v2/job/subscription/detail\",\"text_cn\":\"查看订阅任务详情\",\"text_en\":\"subscription job detail\"}]},{\"name\":\"platform\",\"text_cn\":\"平台管理\",\"text_en\":\"platform\",\"menu_level\":1,\"menu_url\":\"\",\"id\":5,\"parent_id\":0,\"operations\":[]},{\"name\":\"node\",\"text_cn\":\"DTLE节点\",\"text_en\":\"dtle nodes\",\"menu_level\":2,\"menu_url\":\"/node\",\"id\":6,\"parent_id\":5,\"operations\":[{\"action\":\"node-list\",\"uri\":\"/v2/nodes\",\"text_cn\":\"获取节点列表\",\"text_en\":\"get node list\"}]},{\"name\":\"users\",\"admin_only\":true,\"text_cn\":\"用户管理\",\"text_en\":\"user manage\",\"menu_level\":2,\"menu_url\":\"/users\",\"id\":7,\"parent_id\":5,\"operations\":[{\"action\":\"user-list\",\"uri\":\"/v2/user/list\",\"text_cn\":\"查看用户列表\",\"text_en\":\"user list\"},{\"action\":\"user-list_tenant\",\"uri\":\"/v2/tenant/list\",\"text_cn\":\"获取租户列表\",\"text_en\":\"get tenants\"},{\"action\":\"user-create\",\"uri\":\"/v2/user/create\",\"text_cn\":\"创建用户\",\"text_en\":\"create user\"},{\"action\":\"user-delete\",\"uri\":\"/v2/user/delete\",\"text_cn\":\"删除用户\",\"text_en\":\"delete\"},{\"action\":\"user-update\",\"uri\":\"/v2/user/update\",\"text_cn\":\"修改用户\",\"text_en\":\"update user\"}]},{\"name\":\"auth\",\"admin_only\":true,\"text_cn\":\"权限配置\",\"text_en\":\"rights profile\",\"menu_level\":2,\"menu_url\":\"/auth\",\"id\":8,\"parent_id\":5,\"operations\":[{\"action\":\"auth-list\",\"uri\":\"/v2/role/list\",\"text_cn\":\"查看角色列表\",\"text_en\":\"role list\"},{\"action\":\"auth-create\",\"uri\":\"/v2/role/create\",\"text_cn\":\"创建角色\",\"text_en\":\"create\"},{\"action\":\"auth-delete\",\"uri\":\"/v2/role/delete\",\"text_cn\":\"删除角色\",\"text_en\":\"delete\"},{\"action\":\"auth-update\",\"uri\":\"/v2/role/update\",\"text_cn\":\"修改角色\",\"text_en\":\"update\"}]}]"
)

const (
	ControlMsgError  int32 = 1
	ControlMsgFinish int32 = 2
)

var (
	ErrNoConsul = fmt.Errorf("consul return nil value. check if consul is started or reachable")
)

type GencodeType interface {
	Marshal(buf []byte) ([]byte, error)
	Unmarshal(buf []byte) (uint64, error)
	Size() (s uint64)
}

func init() {
	// "Only types that will be transferred as implementations of interface values need to be registered."
	gob.Register(types.BinaryLiteral{})
	if g.EnvIsTrue(g.ENV_BIG_MSG_100K) {
		g.NatsMaxMsg = 100 * 1024 // TODO this does not works
	}
}

type ExecContext struct {
	Subject  string
	StateDir string
}

func ValidateJobName(name string) error {
	if len(name) > g.JobNameLenLimit {
		return fmt.Errorf("job name too long. jobName %v lenLimit %v", name, g.JobNameLenLimit)
	}
	return nil
}

func EncodeTable(v *Table) ([]byte, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func Compress(bs []byte) (outBs []byte, err error) {
	var buf bytes.Buffer
	w, _ := compress.NewWriterLevel(&buf, compress.BestSpeed)
	_, err = w.Write(bs)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func Encode(v GencodeType) ([]byte, error) {
	bs, err := v.Marshal(nil)
	if err != nil {
		return nil, err
	}

	return Compress(bs)
}

func Decode(data []byte, out GencodeType) (err error) {
	r, err := compress.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	msg, err := ioutil.ReadAll(r)
	_ = r.Close()
	if err != nil {
		return err
	}

	n, err := out.Unmarshal(msg)
	if err != nil {
		return err
	}
	if n != uint64(len(msg)) {
		return fmt.Errorf("BinlogEntries.Unmarshal: not all consumed. data: %v, consumed: %v",
			len(msg), n)
	}
	return nil
}
func DecodeMaybeTable(data []byte) (*Table, error) {
	if len(data) > 0 {
		r := &Table{}
		err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(r)
		if err != nil {
			return nil, err
		}
		return r, nil
	} else {
		return nil, nil
	}
}

func DtleParseMysqlGTIDSet(gtidSetStr string) (*mysql.MysqlGTIDSet, error) {
	set0, err := mysql.ParseMysqlGTIDSet(gtidSetStr)
	if err != nil {
		return nil, err
	}

	return set0.(*mysql.MysqlGTIDSet), nil
}

func UpdateGtidSet(gtidSet *mysql.MysqlGTIDSet, sid uuid.UUID, txGno int64) {
	gtidSet.AddSet(mysql.NewUUIDSet(sid, mysql.Interval{
		Start: txGno,
		Stop:  txGno + 1,
	}))
}

// regularly update the task status value by the memory usage
func RegularlyUpdateJobStatus(store *StoreManager, shutdownCh chan struct{}, jobId string) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			lowMemoryStatus := g.GetLowMemoryStatus()
			jobInfo, err := store.GetJobInfo(jobId)
			if err != nil {
				store.logger.Error("get job info err", "jobId", jobId, "err", err)
				continue
			}
			if jobInfo.JobStatus == DtleJobStatusNonPaused && lowMemoryStatus {
				jobInfo.JobStatus = DtleJobStatusStop
			} else if jobInfo.JobStatus == DtleJobStatusStop && !lowMemoryStatus {
				jobInfo.JobStatus = DtleJobStatusNonPaused
			} else {
				continue
			}
			store.logger.Info("update job status", "jobId", jobId, "jobStatus", jobInfo.JobStatus)
			if err = store.SaveJobInfo(*jobInfo); err != nil {
				store.logger.Error("get job info err", "jobId", jobId, "err", err)
				continue
			}
		}
	}
}
