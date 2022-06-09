package extractor

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/oracle/config"

	"github.com/actiontech/dtle/g"
)

type dumper struct {
	*common.Dumper
	db          *config.OracleDB
	snapshotSCN int64
}

func NewDumper(db *config.OracleDB, table *common.Table, chunkSize int64,
	logger g.LoggerType, memory *int64, scn int64) *dumper {
	d := &dumper{
		common.NewDumper(table, chunkSize, logger, memory),
		db,
		scn,
	}
	d.PrepareForDumping = d.prepareForDumping
	d.GetChunkData = d.getChunkData
	return d
}

func (d *dumper) prepareForDumping() error {

	return nil
}

// dumps a specific chunk, reading chunk info from the channel
func (d *dumper) getChunkData() (nRows int64, err error) {
	entry := &common.DumpEntry{
		TableSchema: d.TableSchema,
		TableName:   d.TableName,
	}

	if d.Table.TableRename != "" {
		entry.TableName = d.Table.TableRename
	}
	if d.Table.TableSchemaRename != "" {
		entry.TableSchema = d.Table.TableSchemaRename
	}

	// build query sql
	// SELECT * FROM %s.%s AS OF SCN %d where ROWNUM <= %d
	// minus
	// SELECT * FROM %s.%s AS OF SCN %d where ROWNUM < %d
	defer func() {
		if err != nil {
			entry.Err = err.Error()
		}
		if err == nil && len(entry.ValuesX) == 0 {
			return
		}

		keepGoing := true
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for keepGoing {
			select {
			case <-d.ShutdownCh:
				keepGoing = false
			case d.ResultsChannel <- entry:
				atomic.AddInt64(d.Memory, int64(entry.Size()))
				//d.logger.Debug("*** memory", "memory", atomic.LoadInt64(d.memory))
				keepGoing = false
			case <-timer.C:
				d.Logger.Debug("resultsChannel full. waiting and ping conn")
				errPing := d.db.MetaDataConn.PingContext(context.TODO())
				if errPing != nil {
					d.Logger.Debug("ping query row got error.", "err", errPing)
				}
			}
		}
		d.Logger.Debug("resultsChannel", "n", len(d.ResultsChannel))
	}()

	query := fmt.Sprintf(
		`SELECT * FROM %s.%s AS OF SCN %d where ROWNUM <= %d
		minus 
		SELECT * FROM %s.%s AS OF SCN %d where ROWNUM < %d `,
		d.TableSchema,
		d.TableName,
		d.snapshotSCN,
		(d.Iteration+1)*d.ChunkSize,
		d.TableSchema,
		d.TableName,
		d.snapshotSCN,
		(d.Iteration*d.ChunkSize)+1,
	)

	d.Logger.Debug("getChunkData.", "query", query)

	// this must be increased after building query
	d.Iteration += 1
	rows, err := d.db.MetaDataConn.QueryContext(context.TODO(), query)
	if err != nil {
		newErr := fmt.Errorf("error at select chunk. err: %v", err)
		d.Logger.Error(newErr.Error())
		return 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	scanArgs := make([]interface{}, len(columns)) // tmp use, for casting `values` to `[]interface{}`

	for rows.Next() {
		rowValuesRaw := make([]*[]byte, len(columns))
		for i := range rowValuesRaw {
			scanArgs[i] = &rowValuesRaw[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return 0, err
		}

		entry.ValuesX = append(entry.ValuesX, rowValuesRaw)
	}

	nRows = int64(len(entry.ValuesX))
	d.Logger.Debug("getChunkData.", "n_row", nRows)

	if d.Table.TableRename != "" {
		entry.TableName = d.Table.TableRename
	}
	if d.Table.TableSchemaRename != "" {
		entry.TableSchema = d.Table.TableSchemaRename
	}

	return nRows, nil
}
