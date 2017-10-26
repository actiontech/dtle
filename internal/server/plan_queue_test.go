package server

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"udup/internal/models"
)

func TestNewPlanQueue(t *testing.T) {
	tests := []struct {
		name    string
		want    *PlanQueue
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewPlanQueue()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPlanQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPlanQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_pendingPlan_Wait(t *testing.T) {
	type fields struct {
		plan        *models.Plan
		enqueueTime time.Time
		result      *models.PlanResult
		errCh       chan error
	}
	tests := []struct {
		name    string
		fields  fields
		want    *models.PlanResult
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &pendingPlan{
				plan:        tt.fields.plan,
				enqueueTime: tt.fields.enqueueTime,
				result:      tt.fields.result,
				errCh:       tt.fields.errCh,
			}
			got, err := p.Wait()
			if (err != nil) != tt.wantErr {
				t.Errorf("pendingPlan.Wait() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pendingPlan.Wait() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_pendingPlan_respond(t *testing.T) {
	type fields struct {
		plan        *models.Plan
		enqueueTime time.Time
		result      *models.PlanResult
		errCh       chan error
	}
	type args struct {
		result *models.PlanResult
		err    error
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
			p := &pendingPlan{
				plan:        tt.fields.plan,
				enqueueTime: tt.fields.enqueueTime,
				result:      tt.fields.result,
				errCh:       tt.fields.errCh,
			}
			p.respond(tt.args.result, tt.args.err)
		})
	}
}

func TestPlanQueue_Enabled(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			if got := q.Enabled(); got != tt.want {
				t.Errorf("PlanQueue.Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanQueue_SetEnabled(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	type args struct {
		enabled bool
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
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			q.SetEnabled(tt.args.enabled)
		})
	}
}

func TestPlanQueue_Enqueue(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	type args struct {
		plan *models.Plan
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    PlanFuture
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			got, err := q.Enqueue(tt.args.plan)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlanQueue.Enqueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PlanQueue.Enqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanQueue_Dequeue(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pendingPlan
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			got, err := q.Dequeue(tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlanQueue.Dequeue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PlanQueue.Dequeue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanQueue_Flush(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			q.Flush()
		})
	}
}

func TestPlanQueue_Stats(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	tests := []struct {
		name   string
		fields fields
		want   *QueueStats
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			if got := q.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PlanQueue.Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanQueue_EmitStats(t *testing.T) {
	type fields struct {
		enabled bool
		stats   *QueueStats
		ready   PendingPlans
		waitCh  chan struct{}
		l       sync.RWMutex
	}
	type args struct {
		period time.Duration
		stopCh chan struct{}
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
			q := &PlanQueue{
				enabled: tt.fields.enabled,
				stats:   tt.fields.stats,
				ready:   tt.fields.ready,
				waitCh:  tt.fields.waitCh,
				l:       tt.fields.l,
			}
			q.EmitStats(tt.args.period, tt.args.stopCh)
		})
	}
}

func TestPendingPlans_Len(t *testing.T) {
	tests := []struct {
		name string
		p    PendingPlans
		want int
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Len(); got != tt.want {
				t.Errorf("PendingPlans.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingPlans_Less(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		p    PendingPlans
		args args
		want bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("PendingPlans.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingPlans_Swap(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		p    PendingPlans
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.Swap(tt.args.i, tt.args.j)
		})
	}
}

func TestPendingPlans_Push(t *testing.T) {
	type args struct {
		e interface{}
	}
	tests := []struct {
		name string
		p    *PendingPlans
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.Push(tt.args.e)
		})
	}
}

func TestPendingPlans_Pop(t *testing.T) {
	tests := []struct {
		name string
		p    *PendingPlans
		want interface{}
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Pop(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PendingPlans.Pop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPendingPlans_Peek(t *testing.T) {
	tests := []struct {
		name string
		p    PendingPlans
		want *pendingPlan
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Peek(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PendingPlans.Peek() = %v, want %v", got, tt.want)
			}
		})
	}
}
