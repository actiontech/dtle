package mysql

import (
	"github.com/actiontech/dts/helper/u"
	"github.com/actiontech/dts/internal/config"
	umconf "github.com/actiontech/dts/internal/config/mysql"
	"github.com/actiontech/dts/internal/logger"
	"os"
	"testing"
	"database/sql"
	"time"
)

func TestDumper2000(t *testing.T) {
	logger := logger.NewEntry(logger.New(os.Stdout, logger.DebugLevel))
	var err error

	mysqlCtx := &config.MySQLDriverConfig{
		ConnectionConfig: &umconf.ConnectionConfig{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
			Password: "password",
			Charset: "utf8mb4",
		},
	}
	mysqlCtx.SetDefault()

	i := NewInspector(mysqlCtx, logger)
	u.PanicIfErr(i.InitDBConnections())
	table := config.NewTable("tpcc1", "order_line")
	i.ValidateOriginalTable("tpcc1", "order_line", table)

	logger.Infof("unique_key: %v", table.UseUniqueKey)

	db, err := sql.Open("mysql", "root:password@tcp(127.0.0.1:3307)/")
	u.PanicIfErr(err)
	tx, err := db.Begin()
	u.PanicIfErr(err)
	_, err = tx.Exec("start transaction with consistent snapshot")
	u.PanicIfErr(err)

	d := NewDumper(tx, table, 2000, logger)

	go func() {
		for range d.resultsChannel {
			time.Sleep(50 * time.Millisecond)
		}
	}()

	d.Dump()

	end := make(chan struct{})
	<-end
}
