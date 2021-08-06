package common

import (
	"bytes"
	compress "compress/gzip"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/actiontech/dtle/g"
	"github.com/pingcap/tidb/types"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
)

const (
	DefaultConnectWaitSecond = 10
	DefaultConnectWait       = DefaultConnectWaitSecond * time.Second

	DtleJobStatusNonPaused   = "non-paused"
	DtleJobStatusPaused      = "paused"
	DtleJobStatusUndefined   = "undefined"
	DtleJobStatusReverseInit = "reverse-init"
	TargetGtidFinished       = "finished"
)

const (
	TaskStateComplete int = iota
	TaskStateRestart
	TaskStateDead
)

const (
	DefaultAdminTenant = "platform"
	DefaultAdminUser   = "admin"
	DefaultAdminPwd    = "admin"
	DefaultRole        = "admin"
	DefaultAdminAuth   = "{\"sync\":[{\"action\":\"list\",\"uri\":\"/v2/jobs\"},{\"action\":\"pause\",\"uri\":\"/v2/job/pause\"},{\"action\":\"resume\",\"uri\":\"/v2/job/resume\"},{\"action\":\"reverse_start\",\"uri\":\"/v2/job/reverse_start\"},{\"action\":\"reverse\",\"uri\":\"/v2/job/reverse\"},{\"action\":\"validation\",\"uri\":\"/v2/validation/job\"},{\"action\":\"schemas\",\"uri\":\"/v2/mysql/schemas\"},{\"action\":\"columns\",\"uri\":\"/v2/mysql/columns\"},{\"action\":\"connection\",\"uri\":\"/v2/mysql/instance_connection\"},{\"action\":\"create\",\"uri\":\"/v2/job/migration\"},{\"action\":\"update\",\"uri\":\"/v2/job/migration\"},{\"action\":\"detail\",\"uri\":\"/v2/job/migration/detail\"}],\"migration\":[{\"action\":\"list\",\"uri\":\"/v2/jobs\"},{\"action\":\"pause\",\"uri\":\"/v2/job/pause\"},{\"action\":\"resume\",\"uri\":\"/v2/job/resume\"},{\"action\":\"delete\",\"uri\":\"/v2/job/delete\"},{\"action\":\"reverse_start\",\"uri\":\"/v2/job/reverse_start\"},{\"action\":\"reverse\",\"uri\":\"/v2/job/reverse\"},{\"action\":\"validation\",\"uri\":\"/v2/validation/job\"},{\"action\":\"schemas\",\"uri\":\"/v2/mysql/schemas\"},{\"action\":\"columns\",\"uri\":\"/v2/mysql/columns\"},{\"action\":\"connection\",\"uri\":\"/v2/mysql/instance_connection\"},{\"action\":\"create\",\"uri\":\"/v2/job/migration\"},{\"action\":\"update\",\"uri\":\"/v2/job/migration\"},{\"action\":\"detail\",\"uri\":\"/v2/job/migration/detail\"}],\"subscription\":[{\"action\":\"list\",\"uri\":\"/v2/jobs\"},{\"action\":\"pause\",\"uri\":\"/v2/job/pause\"},{\"action\":\"resume\",\"uri\":\"/v2/job/resume\"},{\"action\":\"delete\",\"uri\":\"/v2/job/delete\"},{\"action\":\"reverse_start\",\"uri\":\"/v2/job/reverse_start\"},{\"action\":\"reverse\",\"uri\":\"/v2/job/reverse\"},{\"action\":\"validation\",\"uri\":\"/v2/validation/job\"},{\"action\":\"schemas\",\"uri\":\"/v2/mysql/schemas\"},{\"action\":\"columns\",\"uri\":\"/v2/mysql/columns\"},{\"action\":\"connection\",\"uri\":\"/v2/mysql/instance_connection\"},{\"action\":\"create\",\"uri\":\"/v2/job/subscription\"},{\"action\":\"update\",\"uri\":\"/v2/job/migration\"},{\"action\":\"detail\",\"uri\":\"/v2/job/subscription/detail\"}],\"user\":[{\"action\":\"list\",\"uri\":\"/v2/user/list\"},{\"action\":\"list_tenant\",\"uri\":\"/v2/tenant/list\"},{\"action\":\"create\",\"uri\":\"/v2/user/create\"},{\"action\":\"update\",\"uri\":\"/v2/user/update\"},{\"action\":\"reset_pwd\",\"uri\":\"/v2/user/reset_password\"},{\"action\":\"delete\",\"uri\":\"/v2/user/delete\"},{\"action\":\"current_user\",\"uri\":\"/v2/user/current_user\"},{\"action\":\"list_action\",\"uri\":\"/v2/user/list_action\"},{\"action\":\"create\",\"uri\":\"/v2/user/create\"}],\"role\":[{\"action\":\"list\",\"uri\":\"/v2/role/list\"},{\"action\":\"create\",\"uri\":\"/v2/role/create\"},{\"action\":\"delete\",\"uri\":\"/v2/role/delete\"},{\"action\":\"update\",\"uri\":\"/v2/role/update\"}],\"node\":[{\"action\":\"list\",\"uri\":\"/v2/nodes\"}],\"task\":[{\"action\":\"monitor\",\"uri\":\"/v2/monitor/task\"}]}"
	// TODO: Using configuration to set jwt secret
	JWTSecret = "secret"
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
