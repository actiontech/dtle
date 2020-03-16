/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"
	"github.com/actiontech/dts/api"
)

func Test_formatKV(t *testing.T) {
	type args struct {
		in []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatKV(tt.args.in); got != tt.want {
				t.Errorf("formatKV() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatList(t *testing.T) {
	type args struct {
		in []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatList(tt.args.in); got != tt.want {
				t.Errorf("formatList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_limit(t *testing.T) {
	type args struct {
		s      string
		length int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := limit(tt.args.s, tt.args.length); got != tt.want {
				t.Errorf("limit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatTime(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatTime(tt.args.t); got != tt.want {
				t.Errorf("formatTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatUnixNanoTime(t *testing.T) {
	type args struct {
		nano int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatUnixNanoTime(tt.args.nano); got != tt.want {
				t.Errorf("formatUnixNanoTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatTimeDifference(t *testing.T) {
	type args struct {
		first  time.Time
		second time.Time
		d      time.Duration
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatTimeDifference(tt.args.first, tt.args.second, tt.args.d); got != tt.want {
				t.Errorf("formatTimeDifference() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getLocalNodeID(t *testing.T) {
	type args struct {
		client *api.Client
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getLocalNodeID(tt.args.client)
			if (err != nil) != tt.wantErr {
				t.Errorf("getLocalNodeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getLocalNodeID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_evalFailureStatus(t *testing.T) {
	type args struct {
		eval *api.Evaluation
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := evalFailureStatus(tt.args.eval)
			if got != tt.want {
				t.Errorf("evalFailureStatus() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("evalFailureStatus() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestNewLineLimitReader(t *testing.T) {
	type args struct {
		r           io.ReadCloser
		lines       int
		searchLimit int
		timeLimit   time.Duration
	}
	tests := []struct {
		name string
		args args
		want *LineLimitReader
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewLineLimitReader(tt.args.r, tt.args.lines, tt.args.searchLimit, tt.args.timeLimit); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLineLimitReader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLineLimitReader_Read(t *testing.T) {
	type fields struct {
		ReadCloser  io.ReadCloser
		lines       int
		searchLimit int
		timeLimit   time.Duration
		lastRead    time.Time
		buffer      *bytes.Buffer
		bufFiled    bool
		foundLines  bool
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantN   int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LineLimitReader{
				ReadCloser:  tt.fields.ReadCloser,
				lines:       tt.fields.lines,
				searchLimit: tt.fields.searchLimit,
				timeLimit:   tt.fields.timeLimit,
				lastRead:    tt.fields.lastRead,
				buffer:      tt.fields.buffer,
				bufFiled:    tt.fields.bufFiled,
				foundLines:  tt.fields.foundLines,
			}
			gotN, err := l.Read(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("LineLimitReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("LineLimitReader.Read() = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}
