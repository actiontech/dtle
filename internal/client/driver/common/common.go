package common

import (
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"time"
)

const (
	// DefaultConnectWait is the default timeout used for the connect operation
	DefaultConnectWaitSecond = 10
	DefaultConnectWait = DefaultConnectWaitSecond * time.Second
	DefaultBigTX = 1024 * 1024 * 100
)

type ExecContext struct {
	Subject    string
	Tp         string
	MaxPayload int
	StateDir   string
}

func DtleParseMysqlGTIDSet(gtidSetStr string) (*mysql.MysqlGTIDSet, error) {
	set0, err := mysql.ParseMysqlGTIDSet(gtidSetStr)
	if err != nil {
		return nil, err
	}

	return set0.(*mysql.MysqlGTIDSet), nil
}

func UpdateGtidSet(gtidSet *mysql.MysqlGTIDSet, sidStr string, sid uuid.UUID, txGno int64) {
	slice := mysql.IntervalSlice{mysql.Interval{
		Start: txGno,
		Stop:  txGno + 1,
	}}

	// It seems they all use lower case for uuid.
	uuidSet, ok := gtidSet.Sets[sidStr]
	if !ok {
		gtidSet.AddSet(&mysql.UUIDSet{
			SID:       sid,
			Intervals: slice,
		})
	} else {
		uuidSet.AddInterval(slice)
	}
}
