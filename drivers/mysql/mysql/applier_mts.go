package mysql

import (
	"container/heap"
	"github.com/actiontech/dtle/drivers/mysql/common"
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
}

//  shutdownCh: close to indicate a shutdown
func NewMtsManager(shutdownCh chan struct{}) *MtsManager {
	return &MtsManager{
		lastCommitted: 0,
		updated:       make(chan struct{}, 1), // 1-buffered, see #211-7.1
		shutdownCh:    shutdownCh,
		m:             nil,
		chExecuted:    make(chan int64),
	}
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
