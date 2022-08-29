/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package common

import (
	"context"
	"sync"

	"github.com/actiontech/dtle/g"
)

type GetChunkDataFn func() (nRows int64, err error)
type PrepareFn func() (err error)

type Dumper struct {
	Ctx                context.Context
	Logger             g.LoggerType
	ChunkSize          int64
	TableSchema        string
	EscapedTableSchema string
	TableName          string
	EscapedTableName   string
	Table              *Table
	Iteration          int64
	Columns            string
	ResultsChannel     chan *DumpEntry
	shutdown           bool
	ShutdownCh         chan struct{}
	shutdownLock       sync.Mutex

	Memory *int64

	GetChunkData      GetChunkDataFn
	PrepareForDumping PrepareFn
}

func NewDumper(ctx context.Context, table *Table, chunkSize int64, logger g.LoggerType, memory *int64) *Dumper {

	dumper := &Dumper{
		Ctx:            ctx,
		Logger:         logger,
		TableSchema:    table.TableSchema,
		TableName:      table.TableName,
		Table:          table,
		ResultsChannel: make(chan *DumpEntry, 24),
		ChunkSize:      chunkSize,
		ShutdownCh:     make(chan struct{}),
		Memory:         memory,
	}
	return dumper
}

func (d *Dumper) Dump() error {
	err := d.PrepareForDumping()
	if err != nil {
		return err
	}

	go func() {
		defer close(d.ResultsChannel)
		for {
			select {
			case <-d.ShutdownCh:
				return
			default:
			}

			nRows, err := d.GetChunkData()
			if err != nil {
				d.Logger.Error("error at dump", "err", err)
				break
			}

			if nRows < d.ChunkSize {
				// If nRows < d.chunkSize while there are still more rows, it is a possible mysql bug.
				d.Logger.Info("nRows < d.chunkSize.", "nRows", nRows, "chunkSize", d.ChunkSize)
			}
			if nRows == 0 {
				d.Logger.Info("nRows == 0. dump finished.", "nRows", nRows, "chunkSize", d.ChunkSize)
				break
			}
		}
	}()

	return nil
}

func (d *Dumper) Close() error {
	// Quit goroutine
	d.shutdownLock.Lock()
	defer d.shutdownLock.Unlock()

	if d.shutdown {
		return nil
	}
	d.shutdown = true
	close(d.ShutdownCh)
	return nil
}
