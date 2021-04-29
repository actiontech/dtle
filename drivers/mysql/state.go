package mysql

import (
	"fmt"
	"strings"
	"sync"

	"github.com/actiontech/dtle/drivers/mysql/common"
)

type taskStore struct {
	store map[string]*taskHandle
	lock  sync.RWMutex
}

func newTaskStore() *taskStore {
	return &taskStore{store: map[string]*taskHandle{}}
}

func (ts *taskStore) Set(id string, handle *taskHandle) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	ts.store[id] = handle
}

func (ts *taskStore) Get(id string) (*taskHandle, bool) {
	ts.lock.RLock()
	defer ts.lock.RUnlock()
	t, ok := ts.store[id]
	return t, ok
}

func (ts *taskStore) Delete(id string) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	delete(ts.store, id)
}

// used by http api
type TaskStoreForApi struct {
	store map[string]*taskHandle
	lock  sync.RWMutex
}

// used by http api
var AllocIdTaskNameToTaskHandler *TaskStoreForApi

func newTaskStoreForApi() *TaskStoreForApi {
	return &TaskStoreForApi{store: map[string]*taskHandle{}}
}

// it can only use allocId and taskName to identify the task from http api
// but internal operation like deleting task need taskId to identify the task
func (ts *TaskStoreForApi) Set(allocId, taskName, taskId string, handle *taskHandle) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	key := fmt.Sprintf("%v-%v-%v", allocId, taskName, taskId)
	ts.store[key] = handle
}

// it can only use allocId and taskName to identify the task from http api
func (ts *TaskStoreForApi) GetTaskStatistics(allocId, taskName string) (*common.TaskStatistics, bool, error) {
	ts.lock.RLock()
	defer ts.lock.RUnlock()

	prefix := fmt.Sprintf("%v-%v-", allocId, taskName)
	for k, _ := range ts.store {
		if strings.HasPrefix(k, prefix) {
			t, ok := ts.store[k]

			stats, err := t.runner.Stats()
			if nil != err {
				return nil, false, fmt.Errorf("get stats from taskHandle failed: %v", err)
			}
			return stats, ok, nil
		}
	}
	return nil, false, nil
}

func (ts *TaskStoreForApi) Delete(id string) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	suffix := fmt.Sprintf("-%v", id)
	for k, _ := range ts.store {
		if strings.HasSuffix(k, suffix) {
			delete(ts.store, k)
		}
	}
}
