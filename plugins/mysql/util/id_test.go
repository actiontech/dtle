package util

import (
	"github.com/ngaut/log"
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
}
