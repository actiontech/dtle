/*
 * Copyright (C) 2016-2018. ActionTech.
 * based on github.com/hashicorp/nomad, github.com/github/gh-ost
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

func main() {
	host := flag.String("host", "127.0.0.1", "Hostname of mysqld")
	port := flag.Int("port", 3306, "Port")
	user := flag.String("user", "root", "User")
	password := flag.String("password", "password", "Password")
	gtidSet := flag.String("gtid", "", "GtidSet")

	flag.Parse()

	var err error

	rand.Seed(time.Now().Unix())
	syncerConf := replication.BinlogSyncerConfig{
		ServerID: rand.Uint32(),
		Flavor:   "mysql",
		Host:     *host,
		Port:     uint16(*port),
		User:     *user,
		Password: *password,
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/", *user, *password, *host, *port))
	panicIfErr(err)

	if *gtidSet == "" {
		var dummy interface{}
		err = db.QueryRow("show master status").Scan(&dummy, &dummy, &dummy, &dummy, gtidSet)
		panicIfErr(err)
	}

	fmt.Printf("ExecutedGtidSet: %v\n", *gtidSet)

	gtid, err := mysql.ParseMysqlGTIDSet(*gtidSet)
	panicIfErr(err)

	syncer := replication.NewBinlogSyncer(&syncerConf)
	streamer, err := syncer.StartSyncGTID(gtid)
	panicIfErr(err)

	var lc int64 = 0
	nTx := 0
	nTxTotal := 0

	printAndClear := func() {
		fmt.Printf("lc: %v\tnTxOfThisLc: %v\ttotalTx: %v\n", lc, nTx, nTxTotal)
		nTx = 0
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			syncer.Close()
			printAndClear()
			break
		}
	}()

	for {
		event, err := streamer.GetEvent(context.Background())
		if err == replication.ErrSyncClosed {
			break
		}
		panicIfErr(err)

		switch event.Header.EventType {
		case replication.GTID_EVENT:
			evt, ok := event.Event.(*replication.GTIDEventV57)
			if !ok {
				panic("not GTIDEventV57")
			}
			nTxTotal += 1
			if evt.GTID.LastCommitted > lc {
				printAndClear()
				lc = evt.GTID.LastCommitted
			}

			nTx += 1
		default:
			// do nothing
		}
	}
}

func panicIfErr(err interface{}) {
	if err != nil {
		panic(err)
	}
}
