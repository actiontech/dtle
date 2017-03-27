package util

import (
	"github.com/ngaut/log"
	"strconv"
	"testing"
)

func TestID(t *testing.T) {
	id, err := NewIdWorker(2, 3, SnsEpoch)
	if err != nil {
		log.Errorf("NewIdWorker(0, 0) error(%v)", err)
		t.FailNow()
	}
	sid, err := id.NextId()
	if err != nil {
		log.Errorf("id.NextId() error(%v)", err)
		t.FailNow()
	}
	log.Infof("snowflake id: %d", sid)

	bid := []byte(strconv.FormatUint(uint64(sid), 10))
	uid, err := strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		log.Errorf(err.Error())
		t.FailNow()
	}
	log.Infof("snowflake id: %d", uint32(uid))
}
