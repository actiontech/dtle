package mysql

import (
	"container/heap"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
	"hash/fnv"
	"sync/atomic"
)

// from container/heap/example_intheap_test.go
type Int64PriQueue []int64

func (q Int64PriQueue) Len() int           { return len(q) }
func (q Int64PriQueue) Less(i, j int) bool { return q[i] < q[j] }
func (q Int64PriQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *Int64PriQueue) Push(x interface{}) {
	*q = append(*q, x.(int64))
}
func (q *Int64PriQueue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

type MtsManager struct {
	// If you set lastCommit = N, all tx with seqNum <= N must have been executed
	lastCommitted int64
	lastEnqueue   int64
	updated       chan struct{}
	shutdownCh    chan struct{}
	// We cannot update LC to a seqNum until all its predecessors has been executed.
	// We put the pending seqNums here.
	m          Int64PriQueue
	chExecuted chan int64
	forceMts   bool
}

//  shutdownCh: close to indicate a shutdown
func NewMtsManager(shutdownCh chan struct{}, logger hclog.Logger) *MtsManager {
	mm := &MtsManager{
		lastCommitted: 0,
		updated:       make(chan struct{}, 1), // 1-buffered, see #211-7.1
		shutdownCh:    shutdownCh,
		m:             nil,
		chExecuted:    make(chan int64),
		forceMts:      g.EnvIsTrue(g.ENV_FORCE_MTS),
	}
	if mm.forceMts {
		logger.Warn("force mts enabled")
	}
	return mm
}

//  This function must be called sequentially.
func (mm *MtsManager) WaitForAllCommitted() bool {
	for {
		if mm.lastCommitted == mm.lastEnqueue {
			return true
		}

		select {
		case <-mm.shutdownCh:
			return false
		case <-mm.updated:
			// continue
		}
	}
}

// block for waiting. return true for can_execute, false for abortion.
//  This function must be called sequentially.
func (mm *MtsManager) WaitForExecution(binlogEntry *common.BinlogEntry) bool {
	mm.lastEnqueue = binlogEntry.Coordinates.SeqenceNumber

	if mm.forceMts {
		return true
	}

	for {
		currentLC := atomic.LoadInt64(&mm.lastCommitted)
		if currentLC >= binlogEntry.Coordinates.LastCommitted {
			return true
		}

		// block until lastCommitted updated
		select {
		case <-mm.shutdownCh:
			return false
		case <-mm.updated:
			// continue
		}
	}
}

func (mm *MtsManager) LcUpdater() {
	for {
		select {
		case <-mm.shutdownCh:
			return

		case seqNum := <-mm.chExecuted:
			if seqNum <= mm.lastCommitted {
				// ignore it
			} else {
				heap.Push(&mm.m, seqNum)

				// update LC to max-continuous-executed
				for mm.m.Len() > 0 {
					least := mm.m[0]
					if least == mm.lastCommitted+1 {
						heap.Pop(&mm.m)
						atomic.AddInt64(&mm.lastCommitted, 1)
						select {
						case mm.updated <- struct{}{}:
						default: // non-blocking
						}
					} else {
						break
					}
				}
			}
		}
	}
}

func (mm *MtsManager) Executed(binlogEntry *common.BinlogEntry) {
	mm.chExecuted <- binlogEntry.Coordinates.SeqenceNumber
}

// HashTx returns an empty slice if there is no row events (DDL TX),
// or there is a row event refering to a no-PK table.
func HashTx(entryCtx *common.BinlogEntryContext) (hashes []uint64) {
	entry := entryCtx.Entry
	for i := range entry.Events {
		event := &entry.Events[i]
		if event.DML == common.NotDML {
			continue
		}
		cols := entryCtx.TableItems[i].Columns

		if len(cols.PKIndex()) == 0 {
			return []uint64{}
		}

		addHash := func(values *common.ColumnValues) {
			h := fnv.New64()
			// hash.WriteXXX never fails
			_, _ = h.Write([]byte(event.DatabaseName))
			_, _ = h.Write(g.HASH_STRING_SEPARATOR_BYTES)
			_, _ = h.Write([]byte(event.TableName))

			for _, pki := range cols.PKIndex() {
				_, _ = h.Write(g.HASH_STRING_SEPARATOR_BYTES)
				_, _ = h.Write(values.BytesColumn(pki))
			}

			hashVal := h.Sum64()
			hashes = append(hashes, hashVal)
		}

		if event.WhereColumnValues != nil {
			addHash(event.WhereColumnValues)
		}
		if event.NewColumnValues != nil {
			addHash(event.NewColumnValues)
		}
	}

	return hashes
}
type WritesetManager struct {
	history          map[uint64]int64
	lastCommonParent int64
	dependencyHistorySize int
}
func NewWritesetManager(historySize int) *WritesetManager {
	return &WritesetManager{
		history:          make(map[uint64]int64),
		lastCommonParent: 0,
		dependencyHistorySize: historySize,
	}
}
func (wm *WritesetManager) GatLastCommit(entryCtx *common.BinlogEntryContext) int64 {
	lastCommit := wm.lastCommonParent
	hashes := HashTx(entryCtx)

	entry := entryCtx.Entry

	exceedsCapacity := false
	canUseWritesets := len(hashes) != 0

	if canUseWritesets {
		exceedsCapacity = len(wm.history)+len(hashes) > wm.dependencyHistorySize
		for _, hash := range hashes {
			if seq, exist := wm.history[hash]; exist {
				if seq > lastCommit && seq < entry.Coordinates.SeqenceNumber {
					lastCommit = seq
				}
			}
			// It might be a big-TX. We strictly limit the size of history.
			if !exceedsCapacity {
				wm.history[hash] = entry.Coordinates.SeqenceNumber
			}
		}
	}
	if exceedsCapacity || !canUseWritesets {
		wm.history = make(map[uint64]int64)
		wm.lastCommonParent = entry.Coordinates.SeqenceNumber
	}

	return lastCommit
}

func (wm *WritesetManager) onRotate() {
	wm.history = make(map[uint64]int64)
	wm.lastCommonParent = 0
}
