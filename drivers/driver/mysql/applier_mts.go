package mysql

import (
	"container/heap"
	"hash/fnv"
	"sync/atomic"

	"github.com/actiontech/dtle/drivers/driver/common"
	"github.com/actiontech/dtle/g"
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
func NewMtsManager(shutdownCh chan struct{}, logger g.LoggerType) *MtsManager {
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
	g.Logger.Debug("WaitForAllCommitted", "lc", mm.lastCommitted, "le", mm.lastEnqueue)
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
func (mm *MtsManager) WaitForExecution(binlogEntry *common.DataEntry) bool {
	mm.lastEnqueue = binlogEntry.Coordinates.GetSequenceNumber()

	if mm.forceMts {
		return true
	}

	for {
		currentLC := atomic.LoadInt64(&mm.lastCommitted)
		if currentLC >= binlogEntry.Coordinates.(*common.MySQLCoordinateTx).LastCommitted {
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
//			g.Logger.Debug("LcUpdater", "seq", seqNum)
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

func (mm *MtsManager) Executed(binlogEntry *common.DataEntry) {
	select {
	case <-mm.shutdownCh:
		return
	case mm.chExecuted <- binlogEntry.Coordinates.GetSequenceNumber():
	}
}

// HashTx returns an empty slice if there is no row events (DDL TX),
// or there is a row event refering to a no-PK table.
func HashTx(entryCtx *common.EntryContext) (hashes []uint64) {
	entry := entryCtx.Entry
	for i := range entry.Events {
		event := &entry.Events[i]
		if event.DML == common.NotDML {
			continue
		}
		cols := entryCtx.TableItems[i].Columns

		if len(cols.UniqueKeys) == 0 {
			g.Logger.Debug("found an event without writesets", "gno", entry.Coordinates.GetGNO(), "i", i)
		}

		for _, uk := range cols.UniqueKeys {
			addPKE := func(row []interface{}) {
				g.Logger.Debug("writeset use key", "name", uk.Name, "columns", uk.Columns.Ordinals)
				h := fnv.New64()
				// hash.WriteXXX never fails
				_, _ = h.Write([]byte(uk.Name))
				_, _ = h.Write([]byte(event.DatabaseName))
				_, _ = h.Write(g.HASH_STRING_SEPARATOR_BYTES)
				_, _ = h.Write([]byte(event.TableName))

				for _, colIndex := range uk.Columns.Ordinals {
					if common.RowColumnIsNull(row, colIndex) {
						return // do not add
					}
					_, _ = h.Write(g.HASH_STRING_SEPARATOR_BYTES)
					_, _ = h.Write(common.RowGetBytesColumn(row, colIndex))
				}

				hashVal := h.Sum64()
				hashes = append(hashes, hashVal)

			}
			for i := range event.Rows {
				addPKE(event.Rows[i])
			}
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
func (wm *WritesetManager) GatLastCommit(entryCtx *common.EntryContext, logger g.LoggerType) int64 {
	entry := entryCtx.Entry
	lastCommit := entry.Coordinates.(*common.MySQLCoordinateTx).LastCommitted

	hashes := HashTx(entryCtx)

	exceedsCapacity := false
	canUseWritesets := len(hashes) != 0

	if canUseWritesets {
		for i := range entry.Events {
			if entry.Events[i].FKParent {
				canUseWritesets = false
				logger.Debug("found fk parent", "gno", entryCtx.Entry.Coordinates.GetGNO())
				break
			}
		}
	}

	if canUseWritesets {
		exceedsCapacity = len(wm.history)+len(hashes) > wm.dependencyHistorySize

		lastCommit = wm.lastCommonParent
		for _, hash := range hashes {
			if seq, exist := wm.history[hash]; exist {
				if seq > lastCommit && seq < entry.Coordinates.GetSequenceNumber() {
					lastCommit = seq
				}
			}
			// It might be a big-TX. We strictly limit the size of history.
			if !exceedsCapacity {
				wm.history[hash] = entry.Coordinates.GetSequenceNumber()
			}
		}
	}
	if exceedsCapacity || !canUseWritesets {
		wm.history = make(map[uint64]int64)
		wm.lastCommonParent = entry.Coordinates.GetSequenceNumber()
	}

	return lastCommit
}

func (wm *WritesetManager) resetCommonParent(seq int64) {
	wm.history = make(map[uint64]int64)
	wm.lastCommonParent = seq
}
