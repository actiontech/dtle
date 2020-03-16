package main

import (
	"context"
	"fmt"
	. "github.com/actiontech/dts/helper/u"
	"github.com/actiontech/dts/utils"
	"github.com/siddontang/go-mysql/mysql"
	rep "github.com/siddontang/go-mysql/replication"
	"math/rand"
	"strconv"
	"time"
)

const (
	MYSQL_USER = "root"
	MYSQL_PASSWORD = "password"
)

type State int

func (s State) String() string {
	switch s {
	case WAIT_TX1: return "WAIT_TX1"
	case READ_TX1: return "READ_TX1"
	case WAIT_TX2: return "WAIT_TX2"
	case READ_TX2: return "READ_TX2"
	default:
		return "<invalid state>"
	}
}

const (
	WAIT_TX1 State = iota
	READ_TX1
	WAIT_TX2
	READ_TX2
)

func main() {
	fmt.Printf("hello\n")
	rand.Seed(time.Now().Unix())

	_, streamer1 := stream("127.0.0.1", 3307, gtid1)
	_, streamer2 := stream("127.0.0.1", 3308, gtid2)

	n1 := 0
	n2 := 0
	getEvent1 := func() *rep.BinlogEvent {
		n1 += 1
		return getEvent(streamer1)
	}
	getEvent2 := func() *rep.BinlogEvent {
		n2 += 1
		return getEvent(streamer2)
	}

	var gno1 int64 = 0
	var gno2 int64 = 0

	var events1 []*rep.BinlogEvent
	var events2 []*rep.BinlogEvent

	state := WAIT_TX1
	cleanupMeta := false
	gnoMatch := false
	for {
		switch state {
		case WAIT_TX1:
			event := getEvent1()
			switch event.Header.EventType {
			case rep.GTID_EVENT:
				gno1 = parseGtidEvent(event)
				Printlnf("1 GTID_EVENT %v", gno1)
				state = READ_TX1
			case rep.HEARTBEAT_EVENT, rep.FORMAT_DESCRIPTION_EVENT, rep.PREVIOUS_GTIDS_EVENT, rep.ROTATE_EVENT:
				// skip
			default:
				panic(fmt.Errorf("unwanted_event %v %v %v", state, n1, event.Header.EventType))
			}

		case READ_TX1:
			event := getEvent1()
			switch event.Header.EventType {
			case rep.XID_EVENT:
				//evt := event.Event.(*rep.XIDEvent)
				Printlnf("xid %v", gno1)
				state = WAIT_TX2
			case rep.QUERY_EVENT:
				query := parseQueryEvent(event)
				if query != "BEGIN" {
					panic(fmt.Errorf("1 QUERY_EVENT_not_begin %v", query))
				}
			case rep.TABLE_MAP_EVENT:
				// skip. It will be in the RowEvents.
			case rep.UPDATE_ROWS_EVENTv2, rep.WRITE_ROWS_EVENTv2, rep.DELETE_ROWS_EVENTv2:
				events1 = append(events1, event)
			case rep.HEARTBEAT_EVENT, rep.FORMAT_DESCRIPTION_EVENT, rep.PREVIOUS_GTIDS_EVENT, rep.ROTATE_EVENT:
				// skip
			default:
				panic(fmt.Errorf("unwanted_event %v %v %v", state, n1, event.Header.EventType))
			}

		case WAIT_TX2:
			event := getEvent2()
			switch event.Header.EventType {
			case rep.GTID_EVENT:
				gno2 = parseGtidEvent(event)
				state = READ_TX2
				Printlnf("2 %v GTID_EVENT gno %v", n2, gno2)
			case rep.HEARTBEAT_EVENT, rep.FORMAT_DESCRIPTION_EVENT, rep.PREVIOUS_GTIDS_EVENT, rep.ROTATE_EVENT:
				// skip
			default:
				panic(fmt.Errorf("unwanted_event %v %v %v", state, n2, event.Header.EventType))
			}

		case READ_TX2:
			event := getEvent2()
			eventType := event.Header.EventType
			if cleanupMeta {
				if eventType == rep.XID_EVENT {
					cleanupMeta = false
					state = WAIT_TX2
				}
			} else {
				switch eventType {
				case rep.XID_EVENT:
					if gnoMatch {
						Printlnf("2 xid_match %v", gno1)
						state = WAIT_TX1
						gnoMatch = false

						compare(events1, events2, gno1)
						events1 = nil
						events2 = nil
					} else {
						Printlnf("2 xid_no_match %v", gno2)
						state = WAIT_TX2
						events2 = nil
					}
				case rep.QUERY_EVENT:
					query := parseQueryEvent(event)
					if query == "BEGIN" {
						// normal TX
					} else {
						Printlnf("2 QUERY_EVENT_not_begin %v", utils.StrLim(query, 20))
						state = WAIT_TX2
					}
				case rep.TABLE_MAP_EVENT:
				case rep.UPDATE_ROWS_EVENTv2, rep.WRITE_ROWS_EVENTv2, rep.DELETE_ROWS_EVENTv2:
					evt := parseRowsEvent(event)
					schemaName := string(evt.Table.Schema)
					tableName := string(evt.Table.Table)
					if schemaName == "dts" && tableName == "gtid_executed_v3" {
						if eventType == rep.DELETE_ROWS_EVENTv2 {
							Printlnf("2 cleanup_meta")
							cleanupMeta = true
						} else {
							gnoStr := BytesToString(evt.Rows[0][2])
							origGno, err := strconv.ParseInt(gnoStr, 10, 64)
							PanicIfErr(err)
							if origGno == gno1 {
								gnoMatch = true
								Printlnf("2 found_gno_match %v", gno1)
							} else {
								panic(fmt.Errorf("2 gno_not_match gno %v gno %v", gno1, origGno))
							}
						}
					} else {
						events2 = append(events2, event)
					}
				case rep.HEARTBEAT_EVENT, rep.FORMAT_DESCRIPTION_EVENT, rep.PREVIOUS_GTIDS_EVENT, rep.ROTATE_EVENT:
					// skip
				default:
					panic(fmt.Errorf("unwanted_event %v %v %v", state, n2, eventType))
				}
			}
		}
	}
	end := make(chan struct{})
	<-end
}

func compare(events1 []*rep.BinlogEvent, events2 []*rep.BinlogEvent, gno int64) {
	nRow1 := 0
	nRow2 := 0
	for _, event := range events1 {
		nRow1 += len(parseRowsEvent(event).Rows)
	}
	for _, event := range events2 {
		nRow2 += len(parseRowsEvent(event).Rows)
	}

	if nRow1 != nRow2 {
		Printlnf(">>>> %v", gno)
		Printlnf("\nevents 1\n")
		for i, event := range events1 {
			walkRowsEvent(event, i)
		}
		Printlnf("\nevents 2\n")
		for i, event := range events2 {
			walkRowsEvent(event, i)
		}
		Printlnf("<<<< %v", gno)

		//panic(fmt.Errorf("compare total_len_not_match %v %v", nRow1, nRow2))
	}
	//if len(events1) != len(events2) {
	//	panic(fmt.Errorf("compare len_not_match %v %v", len(events1), len(events2)))
	//}
}
func walkRowsEvent(event *rep.BinlogEvent, i int) {
	Printlnf("walk_event %v", i)
	Printlnf("type %v", event.Header.EventType)
	evt := parseRowsEvent(event)
	Printlnf("schema_table %v %v", string(evt.Table.Schema), string(evt.Table.Table))
	for iRow, row := range evt.Rows {
		Printlnf("row %v %v", iRow, row)
	}
}

func stream(host string, port uint16, gtidStr string) (*rep.BinlogSyncer, *rep.BinlogStreamer) {
	var err error
	syncConf := rep.BinlogSyncerConfig{
		ServerID: rand.Uint32(),
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     MYSQL_USER,
		Password: MYSQL_PASSWORD,
	}
	g, err := mysql.ParseMysqlGTIDSet(gtidStr)
	PanicIfErr(err)
	syncer := rep.NewBinlogSyncer(syncConf)
	streamer, err := syncer.StartSyncGTID(g)
	PanicIfErr(err)
	return syncer, streamer
}
func getEvent(streamer *rep.BinlogStreamer) *rep.BinlogEvent {
	event, err := streamer.GetEvent(context.Background())
	PanicIfErr(err)
	return event
}

func parseQueryEvent(event *rep.BinlogEvent) string {
	evt := event.Event.(*rep.QueryEvent)
	return string(evt.Query)
}
func parseGtidEvent(event *rep.BinlogEvent) int64 {
	evt := event.Event.(*rep.GTIDEvent)
	return evt.GNO
}
func parseRowsEvent(event *rep.BinlogEvent) *rep.RowsEvent {
	return event.Event.(*rep.RowsEvent)
}
