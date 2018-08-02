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

	"github.com/ugorji/go/codec"
)

func TestNewTimeTable(t *testing.T) {
	type args struct {
		granularity time.Duration
		limit       time.Duration
	}
	tests := []struct {
		name string
		args args
		want *TimeTable
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTimeTable(tt.args.granularity, tt.args.limit); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTimeTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeTable_Serialize(t *testing.T) {
	type fields struct {
		granularity time.Duration
		limit       time.Duration
		table       []TimeTableEntry
		l           sync.RWMutex
	}
	type args struct {
		enc *codec.Encoder
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
			t := &TimeTable{
				granularity: tt.fields.granularity,
				limit:       tt.fields.limit,
				table:       tt.fields.table,
				l:           tt.fields.l,
			}
			if err := t.Serialize(tt.args.enc); (err != nil) != tt.wantErr {
				t.Errorf("TimeTable.Serialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTimeTable_Deserialize(t *testing.T) {
	type fields struct {
		granularity time.Duration
		limit       time.Duration
		table       []TimeTableEntry
		l           sync.RWMutex
	}
	type args struct {
		dec *codec.Decoder
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
			t := &TimeTable{
				granularity: tt.fields.granularity,
				limit:       tt.fields.limit,
				table:       tt.fields.table,
				l:           tt.fields.l,
			}
			if err := t.Deserialize(tt.args.dec); (err != nil) != tt.wantErr {
				t.Errorf("TimeTable.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTimeTable_Witness(t *testing.T) {
	type fields struct {
		granularity time.Duration
		limit       time.Duration
		table       []TimeTableEntry
		l           sync.RWMutex
	}
	type args struct {
		index uint64
		when  time.Time
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
			t := &TimeTable{
				granularity: tt.fields.granularity,
				limit:       tt.fields.limit,
				table:       tt.fields.table,
				l:           tt.fields.l,
			}
			t.Witness(tt.args.index, tt.args.when)
		})
	}
}

func TestTimeTable_NearestIndex(t *testing.T) {
	type fields struct {
		granularity time.Duration
		limit       time.Duration
		table       []TimeTableEntry
		l           sync.RWMutex
	}
	type args struct {
		when time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t := &TimeTable{
				granularity: tt.fields.granularity,
				limit:       tt.fields.limit,
				table:       tt.fields.table,
				l:           tt.fields.l,
			}
			if got := t.NearestIndex(tt.args.when); got != tt.want {
				t.Errorf("TimeTable.NearestIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeTable_NearestTime(t *testing.T) {
	type fields struct {
		granularity time.Duration
		limit       time.Duration
		table       []TimeTableEntry
		l           sync.RWMutex
	}
	type args struct {
		index uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   time.Time
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t := &TimeTable{
				granularity: tt.fields.granularity,
				limit:       tt.fields.limit,
				table:       tt.fields.table,
				l:           tt.fields.l,
			}
			if got := t.NearestTime(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeTable.NearestTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
