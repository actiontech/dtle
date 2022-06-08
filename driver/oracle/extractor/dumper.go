package extractor

import (
	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/driver/oracle/config"

	"github.com/actiontech/dtle/g"
)

type dumper struct {
	*common.Dumper
	db *config.OracleDB
}

func NewDumper(db *config.OracleDB, table *common.Table, chunkSize int64,
	logger g.LoggerType, memory *int64) *dumper {
	return &dumper{
		common.NewDumper(table, chunkSize, logger, memory),
		db,
	}
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

	// build query sql  (SELECT * FROM x.x LIMIT x OFFSET x AS OF SCN 123)

	// offset
	d.Iteration += 1
	// exec sql
	// d.db.MetaDataConn.QueryRowContext()

	return nRows, nil
}
