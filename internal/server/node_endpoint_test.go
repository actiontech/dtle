/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package server

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"github.com/actiontech/dtle/internal/models"
	"github.com/actiontech/dtle/internal/server/store"
)

func TestNode_Register(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeRegisterRequest
		reply *models.NodeUpdateResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.Register(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.Register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_constructNodeServerInfoResponse(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		snap  *store.StateSnapshot
		reply *models.NodeUpdateResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.constructNodeServerInfoResponse(tt.args.snap, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.constructNodeServerInfoResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_Deregister(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeDeregisterRequest
		reply *models.NodeUpdateResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.Deregister(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.Deregister() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_UpdateStatus(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeUpdateStatusRequest
		reply *models.NodeUpdateResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.UpdateStatus(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.UpdateStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_transitionedToReady(t *testing.T) {
	type args struct {
		newStatus string
		oldStatus string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transitionedToReady(tt.args.newStatus, tt.args.oldStatus); got != tt.want {
				t.Errorf("transitionedToReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_Evaluate(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeEvaluateRequest
		reply *models.NodeUpdateResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.Evaluate(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_GetNode(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeSpecificRequest
		reply *models.SingleNodeResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.GetNode(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.GetNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_GetAllocs(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeSpecificRequest
		reply *models.NodeAllocsResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.GetAllocs(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.GetAllocs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_GetClientAllocs(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeSpecificRequest
		reply *models.NodeClientAllocsResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.GetClientAllocs(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.GetClientAllocs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_UpdateJob(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.JobUpdateRequest
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.UpdateJob(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.UpdateJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_UpdateAlloc(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.AllocUpdateRequest
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.UpdateAlloc(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.UpdateAlloc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_batchUpdate(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		future  *batchFuture
		updates []*models.Allocation
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			n.batchUpdate(tt.args.future, tt.args.updates)
		})
	}
}

func TestNode_List(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		args  *models.NodeListRequest
		reply *models.NodeListResponse
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
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			if err := n.List(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("Node.List() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_createNodeEvals(t *testing.T) {
	type fields struct {
		srv          *Server
		updates      []*models.Allocation
		updateFuture *batchFuture
		updateTimer  *time.Timer
		updatesLock  sync.Mutex
	}
	type args struct {
		nodeID    string
		nodeIndex uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		want1   uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{
				srv:          tt.fields.srv,
				updates:      tt.fields.updates,
				updateFuture: tt.fields.updateFuture,
				updateTimer:  tt.fields.updateTimer,
				updatesLock:  tt.fields.updatesLock,
			}
			got, got1, err := n.createNodeEvals(tt.args.nodeID, tt.args.nodeIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("Node.createNodeEvals() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Node.createNodeEvals() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Node.createNodeEvals() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestNewBatchFuture(t *testing.T) {
	tests := []struct {
		name string
		want *batchFuture
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatchFuture(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatchFuture() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchFuture_Wait(t *testing.T) {
	type fields struct {
		doneCh chan struct{}
		err    error
		index  uint64
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
			b := &batchFuture{
				doneCh: tt.fields.doneCh,
				err:    tt.fields.err,
				index:  tt.fields.index,
			}
			if err := b.Wait(); (err != nil) != tt.wantErr {
				t.Errorf("batchFuture.Wait() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_batchFuture_Index(t *testing.T) {
	type fields struct {
		doneCh chan struct{}
		err    error
		index  uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &batchFuture{
				doneCh: tt.fields.doneCh,
				err:    tt.fields.err,
				index:  tt.fields.index,
			}
			if got := b.Index(); got != tt.want {
				t.Errorf("batchFuture.Index() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchFuture_Respond(t *testing.T) {
	type fields struct {
		doneCh chan struct{}
		err    error
		index  uint64
	}
	type args struct {
		index uint64
		err   error
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
			b := &batchFuture{
				doneCh: tt.fields.doneCh,
				err:    tt.fields.err,
				index:  tt.fields.index,
			}
			b.Respond(tt.args.index, tt.args.err)
		})
	}
}
