package api

import (
	"time"
)

// Task is a single process in a task.
type Task struct {
	Type   string
	NodeId string
	Driver string
	Config map[string]interface{}
	Leader bool
	Status string
}

// Configure is used to configure a single k/v pair on
// the task.
func (t *Task) SetConfig(key string, val interface{}) *Task {
	if t.Config == nil {
		t.Config = make(map[string]interface{})
	}
	t.Config[key] = val
	return t
}

// TaskState tracks the current store of a task and events that caused store
// transitions.
type TaskState struct {
	State      string
	Failed     bool
	StartedAt  time.Time
	FinishedAt time.Time
	Events     []*TaskEvent
}

const (
	TaskSetup            = "Task Setup"
	TaskSetupFailure     = "Setup Failure"
	TaskDriverFailure    = "Driver Failure"
	TaskDriverMessage    = "Driver"
	TaskReceived         = "Received"
	TaskFailedValidation = "Failed Validation"
	TaskStarted          = "Started"
	TaskTerminated       = "Terminated"
	TaskKilling          = "Killing"
	TaskKilled           = "Killed"
	TaskRestarting       = "Restarting"
	TaskNotRestarting    = "Not Restarting"
	TaskSiblingFailed    = "Sibling Task Failed"
	TaskSignaling        = "Signaling"
	TaskRestartSignal    = "Restart Signaled"
	TaskLeaderDead       = "Leader Task Dead"
)


type TableStats struct {
	InsertCount int64
	UpdateCount int64
	DelCount    int64
}

type DelayCount struct {
	Num  uint64
	Time uint64
}

type ThroughputStat struct {
	Num  uint64
	Time uint64
}

type Stats struct {
	TableStats     *TableStats
	DelayCount     *DelayCount
	ThroughputStat *ThroughputStat
}

type TaskStatistics struct {
	Stats     *Stats
	Timestamp int64
}

type AllocStatistics struct {
	Tasks map[string]*TaskStatistics
}

// TaskEvent is an event that effects the state of a task and contains meta-data
// appropriate to the events type.
type TaskEvent struct {
	Type             string
	Time             int64
	FailsTask        bool
	RestartReason    string
	SetupError       string
	DriverError      string
	DriverMessage    string
	ExitCode         int
	Signal           int
	Message          string
	KillReason       string
	KillTimeout      time.Duration
	KillError        string
	StartDelay       int64
	DownloadError    string
	ValidationError  string
	DiskLimit        int64
	DiskSize         int64
	FailedSibling    string
	TaskSignalReason string
	TaskSignal       string
}
