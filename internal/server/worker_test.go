package server

import (
	"reflect"
	"sync"
	"testing"
	"time"
	log "udup/internal/logger"
	"udup/internal/models"
	"udup/internal/server/scheduler"
)

func TestNewWorker(t *testing.T) {
	type args struct {
		srv *Server
	}
	tests := []struct {
		name    string
		args    args
		want    *Worker
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewWorker(tt.args.srv)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewWorker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_SetPause(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		p bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			w.SetPause(tt.args.p)
		})
	}
}

func TestWorker_checkPaused(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			w.checkPaused()
		})
	}
}

func TestWorker_run(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			w.run()
		})
	}
}

func TestWorker_dequeueEvaluation(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *models.Evaluation
		want1  string
		want2  bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			got, got1, got2 := w.dequeueEvaluation(tt.args.timeout)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.dequeueEvaluation() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Worker.dequeueEvaluation() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("Worker.dequeueEvaluation() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestWorker_sendAck(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		evalID string
		token  string
		ack    bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			w.sendAck(tt.args.evalID, tt.args.token, tt.args.ack)
		})
	}
}

func TestWorker_waitForIndex(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		index   uint64
		timeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if err := w.waitForIndex(tt.args.index, tt.args.timeout); (err != nil) != tt.wantErr {
				t.Errorf("Worker.waitForIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_invokeScheduler(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		eval  *models.Evaluation
		token string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if err := w.invokeScheduler(tt.args.eval, tt.args.token); (err != nil) != tt.wantErr {
				t.Errorf("Worker.invokeScheduler() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_SubmitPlan(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		plan *models.Plan
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.PlanResult
		want1   scheduler.State
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			got, got1, err := w.SubmitPlan(tt.args.plan)
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.SubmitPlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.SubmitPlan() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Worker.SubmitPlan() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestWorker_UpdateEval(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if err := w.UpdateEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Worker.UpdateEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_CreateEval(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if err := w.CreateEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Worker.CreateEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_ReblockEval(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		eval *models.Evaluation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if err := w.ReblockEval(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("Worker.ReblockEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorker_shouldResubmit(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if got := w.shouldResubmit(tt.args.err); got != tt.want {
				t.Errorf("Worker.shouldResubmit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_backoffErr(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	type args struct {
		base  time.Duration
		limit time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			if got := w.backoffErr(tt.args.base, tt.args.limit); got != tt.want {
				t.Errorf("Worker.backoffErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorker_backoffReset(t *testing.T) {
	type fields struct {
		srv           *Server
		logger        *log.Logger
		start         time.Time
		paused        bool
		pauseLock     sync.Mutex
		pauseCond     *sync.Cond
		failures      uint
		evalToken     string
		snapshotIndex uint64
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				srv:           tt.fields.srv,
				logger:        tt.fields.logger,
				start:         tt.fields.start,
				paused:        tt.fields.paused,
				pauseLock:     tt.fields.pauseLock,
				pauseCond:     tt.fields.pauseCond,
				failures:      tt.fields.failures,
				evalToken:     tt.fields.evalToken,
				snapshotIndex: tt.fields.snapshotIndex,
			}
			w.backoffReset()
		})
	}
}
