package util

import (
	"strconv"
	"testing"

	ulog "udup/logger"
)

func TestID(t *testing.T) {
	id, err := NewIdWorker(2, 3, SnsEpoch)
	if err != nil {
		ulog.Logger.Errorf("NewIdWorker(0, 0) error(%v)", err)
		t.FailNow()
	}
	sid, err := id.NextId()
	if err != nil {
		ulog.Logger.Errorf("id.NextId() error(%v)", err)
		t.FailNow()
	}
	ulog.Logger.Infof("snowflake id: %d", sid)

	bid := []byte(strconv.FormatUint(uint64(sid), 10))
	uid, err := strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		ulog.Logger.Errorf(err.Error())
		t.FailNow()
	}
	ulog.Logger.Infof("snowflake id: %d", uint32(uid))
}
