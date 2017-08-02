package scheduler

import (
	"reflect"
	"testing"
	log "udup/internal/logger"
	"udup/internal/models"
)

func TestSetStatusError_Error(t *testing.T) {
	type fields struct {
		Err        error
		EvalStatus string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SetStatusError{
				Err:        tt.fields.Err,
				EvalStatus: tt.fields.EvalStatus,
			}
			if got := s.Error(); got != tt.want {
				t.Errorf("SetStatusError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewGenericScheduler(t *testing.T) {
	type args struct {
		logger  *log.Logger
		state   State
		planner Planner
	}
	tests := []struct {
		name string
		args args
		want Scheduler
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewGenericScheduler(tt.args.logger, tt.args.state, tt.args.planner); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewGenericScheduler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenericScheduler_Process(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
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
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			if err := s.Process(tt.args.eval); (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.Process() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenericScheduler_createBlockedEval(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
	}
	type args struct {
		planFailure bool
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
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			if err := s.createBlockedEval(tt.args.planFailure); (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.createBlockedEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenericScheduler_process(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			got, err := s.process()
			if (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GenericScheduler.process() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenericScheduler_filterCompleteAllocs(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
	}
	type args struct {
		allocs []*models.Allocation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*models.Allocation
		want1  map[string]*models.Allocation
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			got, got1 := s.filterCompleteAllocs(tt.args.allocs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenericScheduler.filterCompleteAllocs() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("GenericScheduler.filterCompleteAllocs() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGenericScheduler_computeJobAllocs(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
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
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			if err := s.computeJobAllocs(); (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.computeJobAllocs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenericScheduler_computePlacements(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
	}
	type args struct {
		place []allocTuple
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
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			if err := s.computePlacements(tt.args.place); (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.computePlacements() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenericScheduler_findPreferredNode(t *testing.T) {
	type fields struct {
		logger         *log.Logger
		state          State
		planner        Planner
		eval           *models.Evaluation
		job            *models.Job
		plan           *models.Plan
		planResult     *models.PlanResult
		ctx            *EvalContext
		nextEval       *models.Evaluation
		blocked        *models.Evaluation
		failedTGAllocs map[string]*models.AllocMetric
		queuedAllocs   map[string]int
	}
	type args struct {
		allocTuple *allocTuple
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantNode *models.Node
		wantErr  bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &GenericScheduler{
				logger:         tt.fields.logger,
				state:          tt.fields.state,
				planner:        tt.fields.planner,
				eval:           tt.fields.eval,
				job:            tt.fields.job,
				plan:           tt.fields.plan,
				planResult:     tt.fields.planResult,
				ctx:            tt.fields.ctx,
				nextEval:       tt.fields.nextEval,
				blocked:        tt.fields.blocked,
				failedTGAllocs: tt.fields.failedTGAllocs,
				queuedAllocs:   tt.fields.queuedAllocs,
			}
			gotNode, err := s.findPreferredNode(tt.args.allocTuple)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenericScheduler.findPreferredNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotNode, tt.wantNode) {
				t.Errorf("GenericScheduler.findPreferredNode() = %v, want %v", gotNode, tt.wantNode)
			}
		})
	}
}
