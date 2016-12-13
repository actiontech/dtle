package api

import (
	"time"
)

// MemoryStats holds memory usage related stats
type MemoryStats struct {
	RSS            uint64
	Cache          uint64
	Swap           uint64
	MaxUsage       uint64
	KernelUsage    uint64
	KernelMaxUsage uint64
	Measured       []string
}

// CpuStats holds cpu usage related stats
type CpuStats struct {
	SystemMode       float64
	UserMode         float64
	TotalTicks       float64
	ThrottledPeriods uint64
	ThrottledTime    uint64
	Percent          float64
	Measured         []string
}

// ResourceUsage holds information related to cpu and memory stats
type ResourceUsage struct {
	MemoryStats *MemoryStats
	CpuStats    *CpuStats
}

// TaskResourceUsage holds aggregated resource usage of all processes in a Task
// and the resource usage of the individual pids
type TaskResourceUsage struct {
	ResourceUsage *ResourceUsage
	Timestamp     int64
	Pids          map[string]*ResourceUsage
}

// AllocResourceUsage holds the aggregated task resource usage of the
// allocation.
type AllocResourceUsage struct {
	ResourceUsage *ResourceUsage
	Tasks         map[string]*TaskResourceUsage
	Timestamp     int64
}

// RestartPolicy defines how the Udup client restarts
// tasks in a taskgroup when they fail
type RestartPolicy struct {
	Interval time.Duration
	Attempts int
	Delay    time.Duration
	Mode     string
}

// EphemeralDisk is an ephemeral disk object
type EphemeralDisk struct {
	Sticky bool
	SizeMB int `mapstructure:"size"`
}

// TaskGroup is the unit of scheduling.
type TaskGroup struct {
	Name          string
	NodeID        string
	Tasks         []*Task
	RestartPolicy *RestartPolicy
	EphemeralDisk *EphemeralDisk
	Meta          map[string]string
}

// NewTaskGroup creates a new TaskGroup.
func NewTaskGroup(name string) *TaskGroup {
	return &TaskGroup{
		Name: name,
	}
}

// AddMeta is used to add a meta k/v pair to a task group
func (g *TaskGroup) SetMeta(key, val string) *TaskGroup {
	if g.Meta == nil {
		g.Meta = make(map[string]string)
	}
	g.Meta[key] = val
	return g
}

// AddTask is used to add a new task to a task group.
func (g *TaskGroup) AddTask(t *Task) *TaskGroup {
	g.Tasks = append(g.Tasks, t)
	return g
}

// RequireDisk adds a ephemeral disk to the task group
func (g *TaskGroup) RequireDisk(disk *EphemeralDisk) *TaskGroup {
	g.EphemeralDisk = disk
	return g
}

// LogConfig provides configuration for log rotation
type LogConfig struct {
	MaxFiles      int
	MaxFileSizeMB int
}

// Task is a single process in a task group.
type Task struct {
	Name        string
	Driver      string
	Config      map[string]interface{}
	Resources   *Resources
	Meta        map[string]string
	KillTimeout time.Duration
	LogConfig   *LogConfig
}

type Template struct {
	SourcePath    string
	DestPath      string
	EmbededTmpl   string
	ChangeMode    string
	RestartSignal string
	Splay         time.Duration
	Once          bool
}

// NewTask creates and initializes a new Task.
func NewTask(name, driver string) *Task {
	return &Task{
		Name:   name,
		Driver: driver,
	}
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

// SetMeta is used to add metadata k/v pairs to the task.
func (t *Task) SetMeta(key, val string) *Task {
	if t.Meta == nil {
		t.Meta = make(map[string]string)
	}
	t.Meta[key] = val
	return t
}

// Require is used to add resource requirements to a task.
func (t *Task) Require(r *Resources) *Task {
	t.Resources = r
	return t
}

// SetLogConfig sets a log config to a task
func (t *Task) SetLogConfig(l *LogConfig) *Task {
	t.LogConfig = l
	return t
}

// TaskState tracks the current state of a task and events that caused state
// transitions.
type TaskState struct {
	State string
	// Failed marks a task as having failed
	Failed bool

	Events []*TaskEvent
}

const (
	TaskDriverFailure    = "Driver Failure"
	TaskReceived         = "Received"
	TaskFailedValidation = "Failed Validation"
	TaskStarted          = "Started"
	TaskTerminated       = "Terminated"
	TaskKilling          = "Killing"
	TaskKilled           = "Killed"
	TaskRestarting       = "Restarting"
	TaskNotRestarting    = "Not Restarting"
	TaskDiskExceeded     = "Disk Exceeded"
	TaskSiblingFailed    = "Sibling task failed"
)

// TaskEvent is an event that effects the state of a task and contains meta-data
// appropriate to the events type.
type TaskEvent struct {
	Type            string
	Time            int64
	FailsTask       bool
	RestartReason   string
	SetupError      string
	DriverError     string
	ExitCode        int
	Signal          int
	Message         string
	KillReason      string
	KillTimeout     time.Duration
	KillError       string
	StartDelay      int64
	ValidationError string
	DiskLimit       int64
	DiskSize        int64
	FailedSibling   string
}
