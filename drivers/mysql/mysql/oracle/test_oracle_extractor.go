package oracle

import (
	"time"

	"github.com/actiontech/dtle/drivers/mysql/common"
)

func Oracle_test_reader(entriesChannel chan<- *common.BinlogEntryContext) {
	for {
		entriesChannel <- NewTestEntryContext()
		time.Sleep(time.Second * 5)
	}
}
