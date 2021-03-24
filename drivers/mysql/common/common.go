package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/actiontech/dtle/g"
	"github.com/golang/snappy"
	"github.com/pingcap/tidb/types"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"os"
	"time"
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWaitSecond = 10
	DefaultConnectWaitSecondAckLimit = 6
	DefaultConnectWait = DefaultConnectWaitSecond * time.Second
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
	if os.Getenv(g.ENV_BIG_MSG_1M) != "" {
		g.NatsMaxPayload = 1024 * 1024
	}
}

type ExecContext struct {
	Subject    string
	StateDir   string
}

func ValidateJobName(name string) error {
	if len(name) > g.JobNameLenLimit {
		return fmt.Errorf("job name too long. jobName %v lenLimit %v", name, g.JobNameLenLimit)
	}
	return nil
}

func GobEncode(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func Encode(v GencodeType) ([]byte, error) {
	bs, err := v.Marshal(nil)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, bs), nil
}

func Decode(data []byte, out GencodeType) (err error) {
	msg, err := snappy.Decode(nil, data)
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
func GobDecode(data []byte, vPtr interface{}) (err error) {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(vPtr)
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

