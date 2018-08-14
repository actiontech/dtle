package server

import (
	"testing"
	"udup/internal/models"
)

func TestEval_GetEval(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalSpecificRequest
		reply *models.SingleEvalResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.GetEval(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.GetEval() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Dequeue(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalDequeueRequest
		reply *models.EvalDequeueResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Dequeue(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Dequeue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Ack(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalAckRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Ack(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Ack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Nack(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalAckRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Nack(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Nack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Update(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalUpdateRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Update(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Create(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalUpdateRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Create(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Reblock(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalUpdateRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Reblock(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Reblock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Reap(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalDeleteRequest
		reply *models.GenericResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Reap(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Reap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_List(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalListRequest
		reply *models.EvalListResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.List(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.List() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEval_Allocations(t *testing.T) {
	type fields struct {
		srv *Server
	}
	type args struct {
		args  *models.EvalSpecificRequest
		reply *models.EvalAllocationsResponse
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
			e := &Eval{
				srv: tt.fields.srv,
			}
			if err := e.Allocations(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Eval.Allocations() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
