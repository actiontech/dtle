/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	. "github.com/actiontech/dts/helper/u"
	"os"
	"os/signal"

	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

func main() {
	shutdown := make(chan struct{})

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
	PanicIfErr(err)

	if *gtidSet == "" {
		var dummy interface{}
		err = db.QueryRow("show master status").Scan(&dummy, &dummy, &dummy, &dummy, gtidSet)
		PanicIfErr(err)
	}

	fmt.Printf("ExecutedGtidSet: %v\n", *gtidSet)

	gtid, err := mysql.ParseMysqlGTIDSet(*gtidSet)
	PanicIfErr(err)

	syncer := replication.NewBinlogSyncer(syncerConf)
	streamer, err := syncer.StartSyncGTID(gtid)
	PanicIfErr(err)

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
			close(shutdown)
			break
		}
	}()

	eventCh := make(chan *replication.BinlogEvent)

	go func() {
		for {
			event, err := streamer.GetEvent(context.Background())
			if err == replication.ErrSyncClosed {
				break
			}
			PanicIfErr(err)
			eventCh <- event
		}
	}()

	t := time.NewTimer(time.Second)
	keepLoop := true
	for keepLoop {
		t.Reset(2 * time.Second)
		select {
		case event := <-eventCh:
			t.Stop()
			switch event.Header.EventType {
			case replication.GTID_EVENT:
				evt, ok := event.Event.(*replication.GTIDEvent)
				if !ok {
					panic("not GTIDEventV57")
				}
				if evt.LastCommitted > lc {
					if nTx > 0 {
						printAndClear()
					}
					lc = evt.LastCommitted
				}
				nTxTotal += 1
				nTx += 1
			default:
				// do nothing
			}
		case <-t.C:
			if nTx > 0 {
				printAndClear()
			}
		case <-shutdown:
			printAndClear()
			keepLoop = false
		}
	}
}
