package server

import (
	"bytes"
	"io"
	"reflect"
	"sync"
	"testing"
	log "udup/internal/logger"
	"udup/internal/server/store"

	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"
)

func TestNewFSM(t *testing.T) {
	type args struct {
		evalBroker *EvalBroker
		blocked    *BlockedEvals
		logger     *log.Logger
	}
	tests := []struct {
		name          string
		args          args
		want          *udupFSM
		wantLogOutput string
		wantErr       bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logOutput := &bytes.Buffer{}
			got, err := NewFSM(tt.args.evalBroker, tt.args.blocked, logOutput, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFSM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewFSM() = %v, want %v", got, tt.want)
			}
			if gotLogOutput := logOutput.String(); gotLogOutput != tt.wantLogOutput {
				t.Errorf("NewFSM() = %v, want %v", gotLogOutput, tt.wantLogOutput)
			}
		})
	}
}

func Test_udupFSM_Close(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if err := n.Close(); (err != nil) != tt.wantErr {
				t.Errorf("udupFSM.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupFSM_State(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *store.StateStore
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.State(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.State() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_TimeTable(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *TimeTable
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.TimeTable(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.TimeTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_Apply(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		log *raft.Log
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.Apply(tt.args.log); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyUpsertNode(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyUpsertNode(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyUpsertNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyDeregisterNode(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyDeregisterNode(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyDeregisterNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyStatusUpdate(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyStatusUpdate(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyStatusUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyJobStatusUpdate(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyJobStatusUpdate(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyJobStatusUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyUpsertJob(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyUpsertJob(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyUpsertJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyDeregisterJob(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyDeregisterJob(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyDeregisterJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyUpdateEval(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyUpdateEval(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyUpdateEval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyDeleteEval(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyDeleteEval(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyDeleteEval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyAllocUpdate(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyAllocUpdate(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyAllocUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyJobClientUpdate(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyJobClientUpdate(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyJobClientUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_applyAllocClientUpdate(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		buf   []byte
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if got := n.applyAllocClientUpdate(tt.args.buf, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.applyAllocClientUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_Snapshot(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	tests := []struct {
		name    string
		fields  fields
		want    raft.FSMSnapshot
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			got, err := n.Snapshot()
			if (err != nil) != tt.wantErr {
				t.Errorf("udupFSM.Snapshot() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("udupFSM.Snapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_udupFSM_Restore(t *testing.T) {
	type fields struct {
		evalBroker   *EvalBroker
		blockedEvals *BlockedEvals
		logOutput    io.Writer
		logger       *log.Logger
		state        *store.StateStore
		timetable    *TimeTable
		stateLock    sync.RWMutex
	}
	type args struct {
		old io.ReadCloser
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
			n := &udupFSM{
				evalBroker:   tt.fields.evalBroker,
				blockedEvals: tt.fields.blockedEvals,
				logOutput:    tt.fields.logOutput,
				logger:       tt.fields.logger,
				state:        tt.fields.state,
				timetable:    tt.fields.timetable,
				stateLock:    tt.fields.stateLock,
			}
			if err := n.Restore(tt.args.old); (err != nil) != tt.wantErr {
				t.Errorf("udupFSM.Restore() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_Persist(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink raft.SnapshotSink
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.Persist(tt.args.sink); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.Persist() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_persistIndexes(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink    raft.SnapshotSink
		encoder *codec.Encoder
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.persistIndexes(tt.args.sink, tt.args.encoder); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.persistIndexes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_persistNodes(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink    raft.SnapshotSink
		encoder *codec.Encoder
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.persistNodes(tt.args.sink, tt.args.encoder); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.persistNodes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_persistJobs(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink    raft.SnapshotSink
		encoder *codec.Encoder
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.persistJobs(tt.args.sink, tt.args.encoder); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.persistJobs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_persistEvals(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink    raft.SnapshotSink
		encoder *codec.Encoder
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.persistEvals(tt.args.sink, tt.args.encoder); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.persistEvals() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_persistAllocs(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	type args struct {
		sink    raft.SnapshotSink
		encoder *codec.Encoder
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
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			if err := s.persistAllocs(tt.args.sink, tt.args.encoder); (err != nil) != tt.wantErr {
				t.Errorf("udupSnapshot.persistAllocs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_udupSnapshot_Release(t *testing.T) {
	type fields struct {
		snap      *store.StateSnapshot
		timetable *TimeTable
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &udupSnapshot{
				snap:      tt.fields.snap,
				timetable: tt.fields.timetable,
			}
			s.Release()
		})
	}
}
