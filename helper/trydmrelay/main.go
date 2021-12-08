package main

import (
	"context"
	"github.com/actiontech/dtle/helper/u"
	dmconfig "github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	dmlog "github.com/pingcap/dm/pkg/log"
	dmrelay "github.com/pingcap/dm/relay"
	"github.com/pingcap/dm/relay/retry"
	"log"
	"path/filepath"
	"time"

	dmstreamer "github.com/pingcap/dm/pkg/streamer"
)

func main() {
	var err error
	err = dmlog.InitLogger(&dmlog.Config{
		Level:          "Debug",
		Format:         "text",
	})
	u.PanicIfErr(err)
	relayDir := "./binlog"
	relay := dmrelay.NewRelay(&dmrelay.Config{
		EnableGTID:  true,
		AutoFixGTID: false,
		RelayDir:    relayDir,
		ServerID:    666,
		Flavor:      "mysql",
		Charset:     "utf8mb4",
		From:        dmconfig.DBConfig{
			Host:             "10.186.62.40",
			Port:             3307,
			User:             "root",
			Password:         "password",
		},
		BinLogName:  "",
		BinlogGTID:  "acd7d195-06cd-11e9-928f-02000aba3e28:1-105",
		ReaderRetry: retry.ReaderRetryConfig{
			BackoffRollback: 200 * time.Millisecond,
			BackoffMax:      1 * time.Second,
			BackoffMin:      1 * time.Millisecond,
			BackoffJitter:   true,
			BackoffFactor:   2,
		},
	})

	err = relay.Init(context.Background())
	u.PanicIfErr(err)


	end := make(chan struct{})
	go func() {
		pr := relay.Process(context.Background())
		log.Printf("Process() returned %v %v", pr.IsCanceled, len(pr.Errors))
		for _, prErr := range pr.Errors {
			log.Printf("prErr %v", prErr.String())
		}
		end <- struct{}{}
	}()

	go func() {
		for {
			s := relay.Status(nil)
			time.Sleep(1 * time.Second)
			log.Printf("*** status 1 %v", s.(*pb.RelayStatus).GetRelayBinlog())
			log.Printf("*** status 2 %v", s.(*pb.RelayStatus).GetRelayBinlogGtid())
		}
	}()

	go func() {
		for {
			ss, err := dmstreamer.CollectAllBinlogFiles(filepath.Join(relayDir))
			u.PanicIfErr(err)
			for i := range ss {
				log.Printf("file %v", ss[i])
			}

			time.Sleep(1 * time.Second)
		}
	}()

	<-end
}
