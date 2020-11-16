/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package client

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/actiontech/dts/internal/client/driver/common"

	"github.com/armon/go-metrics"

	"github.com/actiontech/dts/internal/client/driver"
	"github.com/actiontech/dts/internal/config"
	"github.com/actiontech/dts/internal/models"
	"github.com/sirupsen/logrus"
)

const (
	// killBackoffBaseline is the baseline time for exponential backoff while
	// killing a task.
	killBackoffBaseline = 5 * time.Second

	// killBackoffLimit is the limit of the exponential backoff for killing
	// the task.
	killBackoffLimit = 2 * time.Minute

	// killFailureLimit is how many times we will attempt to kill a task before
	// giving up and potentially leaking resources.
	killFailureLimit = 5
)

// Worker is used to wrap a task within an allocation and provide the execution context.
type Worker struct {
	config         *config.ClientConfig
	updater        TaskStateUpdater
	logger         *logrus.Logger
	alloc          *models.Allocation
	restartTracker *RestartTracker

	// running marks whether the task is running
	running     bool
	runningLock sync.Mutex

	taskStats     *models.TaskStatistics
	taskStatsLock sync.RWMutex

	task *models.Task

	handle     driver.DriverHandle
	handleLock sync.Mutex

	// payloadRendered tracks whether the payload has been rendered to disk
	payloadRendered bool

	// startCh is used to trigger the start of the task
	startCh chan struct{}

	// unblockCh is used to unblock the starting of the task
	unblockCh   chan struct{}
	unblocked   bool
	unblockLock sync.Mutex

	// restartCh is used to restart a task
	restartCh chan *models.TaskEvent

	destroy      bool
	destroyCh    chan struct{}
	destroyLock  sync.Mutex
	destroyEvent *models.TaskEvent
	workUpdates  chan *models.TaskUpdate

	// waitCh closing marks the run loop as having exited
	waitCh chan struct{}
	// persistLock must be acquired when accessing fields stored by
	// SaveState. SaveState is called asynchronously to TaskRunner.Run by
	// AllocRunner, so all store fields must be synchronized using this
	// lock.
	persistLock sync.Mutex
}

// taskRunnerState is used to snapshot the store of the task runner
type workerState struct {
	Version         string
	Task            *models.Task
	HandleID        string
	PayloadRendered bool
}

// TaskStateUpdater is used to signal that tasks store has changed.
type TaskStateUpdater func(taskName, state string, event *models.TaskEvent)

// NewWorker is used to create a new task context
func NewWorker(logger *logrus.Logger, config *config.ClientConfig,
	updater TaskStateUpdater, alloc *models.Allocation,
	task *models.Task, workUpdates chan *models.TaskUpdate) *Worker {

	// Build the restart tracker.
	t := alloc.Job.LookupTask(alloc.Task)
	if t == nil {
		logger.Errorf("agent: Alloc '%s' for missing task '%s'", alloc.ID, alloc.Task)
		return nil
	}

	restartTracker := newRestartTracker()

	tc := &Worker{
		config:         config,
		updater:        updater,
		logger:         logger,
		restartTracker: restartTracker,
		alloc:          alloc,
		task:           task,
		destroyCh:      make(chan struct{}),
		waitCh:         make(chan struct{}),
		startCh:        make(chan struct{}, 1),
		unblockCh:      make(chan struct{}),
		restartCh:      make(chan *models.TaskEvent),
		workUpdates:    workUpdates,
	}

	return tc
}

// MarkReceived marks the task as received.
func (r *Worker) MarkReceived() {
	r.logger.Debugf("MarkReceived")
	r.updater(r.task.Type, models.TaskStatePending, models.NewTaskEvent(models.TaskReceived))
}

// WaitCh returns a channel to wait for termination
func (r *Worker) WaitCh() <-chan struct{} {
	return r.waitCh
}

// stateFilePath returns the path to our store file
func (r *Worker) stateFilePath() string {
	// Get the MD5 of the task name
	hashVal := md5.Sum([]byte(r.task.Type))
	hashHex := hex.EncodeToString(hashVal[:])
	dirName := fmt.Sprintf("task-%s", hashHex)

	// Generate the path
	path := filepath.Join(r.config.StateDir, "alloc", r.alloc.ID,
		dirName, "store.json")
	return path
}

// SaveState is used to snapshot our store
func (r *Worker) SaveState() error {
	r.persistLock.Lock()
	defer r.persistLock.Unlock()

	r.handleLock.Lock()
	if r.handle != nil {
		id := &config.DriverCtx{}
		handleID := r.handle.ID()
		if err := json.Unmarshal([]byte(handleID), id); err != nil {
			r.logger.WithFields(logrus.Fields{
				"handleId": handleID,
				"err":      err,
			}).Errorf("agent: Failed to parse handle")
		}

		{
			tu := &models.TaskUpdate{
				JobID:    r.alloc.JobID,
				TaskType: r.task.Type,
				NatsAddr: id.DriverConfig.NatsAddr,
			}
			if r.task.Type == models.TaskTypeDest {
				if id.DriverConfig.Gtid != "" {
					tu.Gtid = id.DriverConfig.Gtid
				}
				tu.BinlogFile = id.DriverConfig.BinlogFile
				tu.BinlogPos = id.DriverConfig.BinlogPos
			} else { // TaskTypeSrc
				// nothing yet
			}
			r.workUpdates <- tu
		}

		r.logger.WithFields(logrus.Fields{
			"task":       r.task,
			"configLock": r.task.ConfigLock,
		}).Debugf("Worker.SaveState: lock")

		if id.DriverConfig.NatsAddr == "" {
			r.logger.WithField("current_nats", r.task.Config["NatsAddr"]).Infof(
				"DTLE_BUG_MAYBE. Worker.SaveState(): id.DriverConfig.NatsAddr is empty")
		}
		r.task.ConfigLock.Lock()
		r.task.Config["Gtid"] = id.DriverConfig.Gtid
		r.task.Config["NatsAddr"] = id.DriverConfig.NatsAddr
		r.task.ConfigLock.Unlock()
		r.logger.WithFields(logrus.Fields{
			"task": r.task,
		}).Debugf("Worker.SaveState: after unlock")
	}
	r.handleLock.Unlock()
	return nil
}

// DestroyState is used to cleanup after ourselves
func (r *Worker) DestroyState() error {
	r.persistLock.Lock()
	defer r.persistLock.Unlock()

	return os.RemoveAll(r.stateFilePath())
}

// setState is used to update the store of the task runner
func (r *Worker) setState(state string, event *models.TaskEvent) {
	// Persist our store to disk.
	r.logger.Debugf("setState.SaveState")
	if err := r.SaveState(); err != nil {
		r.logger.WithFields(logrus.Fields{
			"taskType": r.task.Type,
			"err":      err,
		}).Errorf("agent: Failed to save store of Task Runner for task")
	}

	// Indicate the task has been updated.
	r.logger.Debugf("updater")
	r.updater(r.task.Type, state, event)
}

// createDriver makes a driver for the task
func (r *Worker) createDriver() (driver.Driver, error) {
	driverCtx := driver.NewDriverContext(r.task.Type, r.alloc.ID, r.config, r.config.Node, r.logger)
	driver, err := driver.NewDriver(r.task.Driver, driverCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create driver '%s' for alloc %s: %v",
			r.task.Driver, r.alloc.ID, err)
	}
	return driver, err
}

// Run is a long running routine used to manage the task
func (r *Worker) Run() {
	defer close(r.waitCh)
	r.logger.WithFields(logrus.Fields{
		"taskType": r.task.Type,
		"allocId":  r.alloc.ID,
		"taskConfig": r.task.Config,
	}).Debugf("agent: Starting task context from alloc")

	// Create a driver so that we can determine the FSIsolation required
	_, err := r.createDriver()
	if err != nil {
		e := fmt.Errorf("failed to create driver of task %q for alloc %q: %v", r.task.Type, r.alloc.ID, err)
		r.logger.Debugf("setState Run")
		r.setState(
			models.TaskStateDead,
			models.NewTaskEvent(models.TaskSetupFailure).SetSetupError(e).SetFailsTask())
		return
	}

	// Start the run loop
	r.run()

	return
}

// prestart handles life-cycle tasks that occur before the task has started.
func (r *Worker) prestart(resultCh chan bool) {
	// Send the start signal
	select {
	case r.startCh <- struct{}{}:
	default:
	}

	resultCh <- true
}

// run is the main run loop that handles starting the application, destroying
// it, restarts and signals.
func (r *Worker) run() {
	// Predeclare things so we can jump to the RESTART
	var stopCollection chan struct{}
	var handleWaitCh chan *models.WaitResult

	// If we already have a handle, populate the stopCollection and handleWaitCh
	// to fix the invariant that it exists.
	r.handleLock.Lock()
	handleEmpty := r.handle == nil
	r.handleLock.Unlock()

	if !handleEmpty {
		stopCollection = make(chan struct{})
		go r.collectResourceUsageStats(stopCollection)
		handleWaitCh = r.handle.WaitCh()
	}

	for {
		// Do the prestart activities
		prestartResultCh := make(chan bool, 1)
		go r.prestart(prestartResultCh)

	WAIT:
		for {
			select {
			case success := <-prestartResultCh:
				if !success {
					r.logger.Debugf("setState 1")
					r.setState(models.TaskStateDead, nil)
					return
				}
			case <-r.startCh:
				// Start the task if not yet started or it is being forced. This logic
				// is necessary because in the case of a restore the handle already
				// exists.
				r.handleLock.Lock()
				handleEmpty := r.handle == nil
				r.handleLock.Unlock()

				if handleEmpty {
					startErr := r.startTask()
					r.restartTracker.SetStartError(startErr)
					if startErr != nil {
						r.logger.Debugf("setState 2")
						r.setState("", models.NewTaskEvent(models.TaskDriverFailure).SetDriverError(startErr))
						goto RESTART
					}

					// Mark the task as started
					r.logger.Debugf("setState 3")
					r.setState(models.TaskStateRunning, models.NewTaskEvent(models.TaskStarted))
					r.runningLock.Lock()
					r.running = true
					r.runningLock.Unlock()

					if stopCollection == nil {
						stopCollection = make(chan struct{})
						go r.collectResourceUsageStats(stopCollection)
					}

					handleWaitCh = r.handle.WaitCh()
				}

			case waitRes := <-handleWaitCh:
				if waitRes == nil {
					panic("nil wait")
				}

				r.runningLock.Lock()
				r.running = false
				r.runningLock.Unlock()

				// Stop collection of the task's resource usage
				close(stopCollection)

				// Log whether the task was successful or not.
				r.restartTracker.SetWaitResult(waitRes)
				r.logger.Debugf("setState 4")
				r.setState("", r.waitErrorToEvent(waitRes))
				if !waitRes.Successful() {
					r.logger.WithFields(logrus.Fields{
						"taskTuype": r.task.Type,
						"allocId":   r.alloc.ID,
						"waitRes":   waitRes,
					}).Errorf("agent: Task %q for alloc %q failed: %v", r.task.Type, r.alloc.ID, waitRes)
				} else {
					r.logger.WithFields(logrus.Fields{
						"taskType": r.task.Type,
						"allocId":  r.alloc.ID,
					}).Printf("agent: Task %q for alloc %q completed successfully", r.task.Type, r.alloc.ID)
				}

				break WAIT

			case event := <-r.restartCh:
				r.runningLock.Lock()
				running := r.running
				r.runningLock.Unlock()
				common := fmt.Sprintf("task %v for alloc %q", r.task.Type, r.alloc.ID)
				if !running {
					r.logger.Debugf("agent: Skipping restart of %v: task isn't running", common)
					continue
				}

				r.logger.Debugf("agent: Restarting %s: %v", common, event.RestartReason)
				r.logger.Debugf("setState 5")
				r.setState(models.TaskStateRunning, event)
				r.killTask(nil)

				close(stopCollection)

				if handleWaitCh != nil {
					<-handleWaitCh
				}

				// Since the restart isn't from a failure, restart immediately
				// and don't count against the restart policy
				r.restartTracker.SetRestartTriggered()
				break WAIT

			case <-r.destroyCh:
				r.runningLock.Lock()
				running := r.running
				r.runningLock.Unlock()
				if !running {
					r.logger.Debugf("setState 6")
					r.setState(models.TaskStateDead, r.destroyEvent)
					return
				}

				// Store the task event that provides context on the task
				// destroy. The Killed event is set from the alloc_runner and
				// doesn't add detail
				var killEvent *models.TaskEvent
				if r.destroyEvent.Type != models.TaskKilled {
					if r.destroyEvent.Type == models.TaskKilling {
						killEvent = r.destroyEvent
					} else {
						r.logger.Debugf("setState 7")
						r.setState(models.TaskStateRunning, r.destroyEvent)
					}
				}

				r.killTask(killEvent)
				close(stopCollection)
				// Wait for handler to exit before calling cleanup
				<-handleWaitCh

				r.logger.Debugf("setState 8")
				r.setState(models.TaskStateDead, nil)
				return
			}
		}

	RESTART:
		restart := r.shouldRestart()
		if !restart {
			r.logger.Debugf("setState 9")
			r.setState(models.TaskStateDead, nil)
			return
		}

		// Clear the handle so a new driver will be created.
		r.handleLock.Lock()
		r.handle = nil
		handleWaitCh = nil
		stopCollection = nil
		r.handleLock.Unlock()
	}
}

// shouldRestart returns if the task should restart. If the return value is
// true, the task's restart policy has already been considered and any wait time
// between restarts has been applied.
func (r *Worker) shouldRestart() bool {
	state, when := r.restartTracker.GetState()
	reason := r.restartTracker.GetReason()
	switch state {
	case models.TaskNotRestarting, models.TaskTerminated:
		r.logger.WithFields(logrus.Fields{
			"taskType": r.task.Type,
			"allocId":  r.alloc.ID,
		}).Printf("agent: Not restarting task: %v for alloc: %v ", r.task.Type, r.alloc.ID)
		if state == models.TaskNotRestarting {
			r.logger.Debugf("setState restart 1")
			r.setState(models.TaskStateFailed,
				models.NewTaskEvent(models.TaskNotRestarting).
					SetRestartReason(reason).SetFailsTask())
		}
		return false
	case models.TaskRestarting:
		r.logger.WithFields(logrus.Fields{
			"taskType": r.task.Type,
			"allocId":  r.alloc.ID,
			"time":     when,
		}).Printf("agent: Restarting task  for alloc in moment")
		r.logger.Debugf("setState restart 2")
		r.setState(models.TaskStatePending,
			models.NewTaskEvent(models.TaskRestarting).
				SetRestartDelay(when).
				SetRestartReason(reason))
	default:
		r.logger.Errorf("agent: Restart tracker returned unknown store: %q", state)
		return false
	}

	// Sleep but watch for destroy events.
	select {
	case <-time.After(when):
	case <-r.destroyCh:
	}

	// Destroyed while we were waiting to restart, so abort.
	r.destroyLock.Lock()
	destroyed := r.destroy
	r.destroyLock.Unlock()
	if destroyed {
		r.logger.WithFields(logrus.Fields{
			"taskType": r.task.Type,
		}).Debugf("agent: Not restarting task , because it has been destroyed")
		r.logger.Debugf("setState restart 3")
		r.setState(models.TaskStateDead, r.destroyEvent)
		return false
	}

	return true
}

// killTask kills the running task. A killing event can optionally be passed and
// this event is used to mark the task as being killed. It provides a means to
// store extra information.
func (r *Worker) killTask(killingEvent *models.TaskEvent) {
	r.runningLock.Lock()
	running := r.running
	r.runningLock.Unlock()
	if !running {
		return
	}

	// Build the event
	var event *models.TaskEvent
	if killingEvent != nil {
		event = killingEvent
		event.Type = models.TaskKilling
	} else {
		event = models.NewTaskEvent(models.TaskKilling)
	}
	event.SetKillTimeout(models.DefaultKillTimeout)

	// Mark that we received the kill event
	r.logger.Debugf("setState killTask 1")
	r.setState(models.TaskStateRunning, event)

	// Kill the task using an exponential backoff in-case of failures.
	destroySuccess, err := r.handleDestroy()
	if !destroySuccess {
		// We couldn't successfully destroy the resource created.
		r.logger.WithFields(logrus.Fields{
			"taskType": r.task.Type,
			"err":      err,
		}).Errorf("agent: Failed to kill task. Resources may have been leaked")
	}

	r.runningLock.Lock()
	r.running = false
	r.runningLock.Unlock()

	// Store that the task has been destroyed and any associated error.
	r.logger.Debugf("setState killTask 2")
	r.setState("", models.NewTaskEvent(models.TaskKilled).SetKillError(err))
}

// startTask creates the driver, task dir, and starts the task.
func (r *Worker) startTask() error {
	// Create a driver
	drv, err := r.createDriver()
	if err != nil {
		return fmt.Errorf("failed to create driver of task %q for alloc %q: %v",
			r.task.Type, r.alloc.ID, err)
	}

	// Run prestart
	ctx := &common.ExecContext{r.alloc.Job.ID, r.alloc.Job.Type, r.config.MaxPayload, r.config.StateDir}

	// Start the job
	handle, err := drv.Start(ctx, r.task)
	if err != nil {
		wrapped := fmt.Sprintf("Failed to start task %q for alloc %q: %v",
			r.task.Type, r.alloc.ID, err)
		r.logger.WithFields(logrus.Fields{
			"agent": wrapped,
		}).Warnf("")
		return models.WrapRecoverable(wrapped, err)

	}

	r.handleLock.Lock()
	r.handle = handle
	r.handleLock.Unlock()
	return nil
}

// collectResourceUsageStats starts collecting resource usage stats of a Task.
// Collection ends when the passed channel is closed
func (r *Worker) collectResourceUsageStats(stopCollection <-chan struct{}) {
	// start collecting the stats right away and then start collecting every
	// collection interval
	next := time.NewTimer(0)
	defer next.Stop()
	for {
		select {
		case <-next.C:
			next.Reset(r.config.StatsCollectionInterval)
			if r.handle == nil {
				continue
			}
			ru, err := r.handle.Stats()

			if err != nil {
				// Check if the driver doesn't implement stats
				if err.Error() == driver.DriverStatsNotImplemented.Error() {
					r.logger.WithFields(logrus.Fields{
						"taskType": r.task.Type,
						"allocId":  r.alloc.ID,
					}).Debugf("agent: Driver for task  in allocation  doesn't support stats")
					return
				}

				// We do not log when the plugin is shutdown as this is simply a
				// race between the stopCollection channel being closed and calling
				// Stats on the handle.
				if !strings.Contains(err.Error(), "connection is shut down") {
					r.logger.WithFields(logrus.Fields{
						"taskType": r.task.Type,
						"err":      err,
					}).Warnf("agent: Error fetching stats of task")
				}
				continue
			}

			r.taskStatsLock.Lock()
			r.taskStats = ru
			r.taskStatsLock.Unlock()
			if ru != nil {
				r.emitStats(ru)
			}
		case <-stopCollection:
			return
		}
	}
}

// LatestResourceUsage returns the last resource utilization datapoint collected
func (r *Worker) LatestTaskStats() *models.TaskStatistics {
	r.taskStatsLock.RLock()
	defer r.taskStatsLock.RUnlock()
	r.runningLock.Lock()
	defer r.runningLock.Unlock()

	// If the task is not running there can be no latest resource
	if !r.running {
		return nil
	}

	return r.taskStats
}

// handleDestroy kills the task handle. In the case that killing fails,
// handleDestroy will retry with an exponential backoff and will give up at a
// given limit. It returns whether the task was destroyed and the error
// associated with the last kill attempt.
func (r *Worker) handleDestroy() (destroyed bool, err error) {
	// Cap the number of times we attempt to kill the task.
	for i := 0; i < killFailureLimit; i++ {
		if err = r.handle.Shutdown(); err != nil {
			// Calculate the new backoff
			backoff := (1 << (2 * uint64(i))) * killBackoffBaseline
			if backoff > killBackoffLimit {
				backoff = killBackoffLimit
			}

			r.logger.WithFields(logrus.Fields{
				"taskType": r.task.Type,
				"allocId":  r.alloc.ID,
				"backoff":  backoff,
				"err":      err,
			}).Errorf("agent: Failed to kill task  for alloc. Retrying in backoff")
			time.Sleep(time.Duration(backoff))
		} else {
			// Kill was successful
			return true, nil
		}
	}
	return
}

// Restart will restart the task
func (r *Worker) Restart(source, reason string) {
	reasonStr := fmt.Sprintf("%s: %s", source, reason)
	event := models.NewTaskEvent(models.TaskRestartSignal).SetRestartReason(reasonStr)

	select {
	case r.restartCh <- event:
	case <-r.waitCh:
	}
}

// Kill will kill a task and store the error, no longer restarting the task. If
// fail is set, the task is marked as having failed.
func (r *Worker) Kill(source, reason string, fail bool) {
	reasonStr := fmt.Sprintf("%s: %s", source, reason)
	event := models.NewTaskEvent(models.TaskKilling).SetKillReason(reasonStr)
	if fail {
		event.SetFailsTask()
	}

	r.logger.WithFields(logrus.Fields{
		"taskType":  r.task.Type,
		"allocId":   r.alloc.ID,
		"reasonStr": reason,
	}).Debugf("agent: Killing task  for alloc")
	r.Destroy(event)
}

// UnblockStart unblocks the starting of the task. It currently assumes only
// consul-template will unblock
func (r *Worker) UnblockStart(source string) {
	r.unblockLock.Lock()
	defer r.unblockLock.Unlock()
	if r.unblocked {
		return
	}

	r.logger.WithFields(logrus.Fields{
		"taskType": r.task.Type,
		"allocId":  r.alloc.ID,
		"source":   source,
	}).Debugf("agent: Unblocking task for alloc ", r.task.Type, r.alloc.ID, source)
	r.unblocked = true
	close(r.unblockCh)
}

// Helper function for converting a WaitResult into a TaskTerminated event.
func (r *Worker) waitErrorToEvent(res *models.WaitResult) *models.TaskEvent {
	return models.NewTaskEvent(models.TaskTerminated).
		SetExitCode(res.ExitCode).
		SetExitMessage(res.Err)
}

// Destroy is used to indicate that the task context should be destroyed. The
// event parameter provides a context for the destroy.
func (r *Worker) Destroy(event *models.TaskEvent) {
	r.destroyLock.Lock()
	defer r.destroyLock.Unlock()

	if r.destroy {
		return
	}
	r.destroy = true
	r.destroyEvent = event
	close(r.destroyCh)
}

// emitStats emits resource usage stats of tasks to remote metrics collector
// sinks
func (r *Worker) emitStats(ru *models.TaskStatistics) {
	labels := []metrics.Label{{"task_name", fmt.Sprintf("%s_%s", r.alloc.Job.Name, r.alloc.Task)}}
	if r.config.PublishAllocationMetrics {
		metrics.SetGaugeWithLabels([]string{"network", "in_msgs"}, float32(ru.MsgStat.InMsgs), labels)
		metrics.SetGaugeWithLabels([]string{"network", "out_msgs"}, float32(ru.MsgStat.OutMsgs), labels)
		metrics.SetGaugeWithLabels([]string{"network", "in_bytes"}, float32(ru.MsgStat.InBytes), labels)
		metrics.SetGaugeWithLabels([]string{"network", "out_bytes"}, float32(ru.MsgStat.OutBytes), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "src_queue_size"}, float32(ru.BufferStat.ExtractorTxQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "dest_group_queue_size"}, float32(ru.BufferStat.ApplierGroupTxQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "dest_queue_size"}, float32(ru.BufferStat.ApplierTxQueueSize), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_timeout"}, float32(ru.BufferStat.SendByTimeout), labels)
		metrics.SetGaugeWithLabels([]string{"buffer", "send_by_size_full"}, float32(ru.BufferStat.SendBySizeFull), labels)
	}

	metrics.SetGaugeWithLabels([]string{"delay", "time"}, float32(ru.DelayTime), labels)

	if ru.TableStats != nil && r.config.PublishAllocationMetrics {
		metrics.SetGaugeWithLabels([]string{"table", "insert"}, float32(ru.TableStats.InsertCount), labels)
		metrics.SetGaugeWithLabels([]string{"table", "update"}, float32(ru.TableStats.UpdateCount), labels)
		metrics.SetGaugeWithLabels([]string{"table", "delete"}, float32(ru.TableStats.DelCount), labels)
	}

	if ru.DelayCount != nil && r.config.PublishAllocationMetrics {
		metrics.SetGaugeWithLabels([]string{"delay", "num"}, float32(ru.DelayCount.Num), labels)
		metrics.SetGaugeWithLabels([]string{"delay", "time"}, float32(ru.DelayCount.Time), labels)
	}

	if ru.ThroughputStat != nil && r.config.PublishAllocationMetrics {
		metrics.SetGaugeWithLabels([]string{"throughput", "num"}, float32(ru.ThroughputStat.Num), labels)
		metrics.SetGaugeWithLabels([]string{"throughput", "time"}, float32(ru.ThroughputStat.Time), labels)
	}
}
