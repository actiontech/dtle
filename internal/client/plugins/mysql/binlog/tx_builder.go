package binlog

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/issuj/gofaster/base64"
	"github.com/satori/go.uuid"
	binlog "github.com/siddontang/go-mysql/replication"

	uconf "udup/config"
	usql "udup/internal/client/plugins/mysql/sql"
	ulog "udup/logger"
)

const (
	EventsChannelBufferSize = 500000
)

type Transaction_t struct {
	ServerId       string
	StartEventFile string
	StartEventPos  uint32
	EndEventFile   string
	EndEventPos    uint32
	SID            string
	GNO            int64
	//Gtid           string
	hasBeginQuery bool
	// table -> [row]. row is identified by a hash of pk values.
	Impacting    map[uint64]([]string)
	ImpactingAll bool
	Query        []string
	Fde          string
	eventCount   int //for evaluate
	EventSize    uint64
	ErrorCode    uint16
}

func (tx *Transaction_t) addImpact(tableId uint64, rowId string) {
	if tx.Impacting[tableId] == nil {
		tx.Impacting[tableId] = []string{}
	}
	tx.Impacting[tableId] = append(tx.Impacting[tableId], rowId)
}

type TxBuilder struct {
	Cfg     *uconf.DriverConfig
	EvtChan chan *BinlogEvent
	TxChan  chan *Transaction_t

	currentTx          *Transaction_t
	txCount            int
	currentFde         string
	currentSqlB64      *bytes.Buffer
	arrayCurrentSqlB64 []string

	reMap map[string]*regexp.Regexp
}

func NewTxBuilder(cfg *uconf.DriverConfig) *TxBuilder {
	return &TxBuilder{
		Cfg:     cfg,
		EvtChan: make(chan *BinlogEvent, EventsChannelBufferSize),
		TxChan:  make(chan *Transaction_t, 100),
		reMap:   make(map[string]*regexp.Regexp),
	}
}

func (tb *TxBuilder) Run() {
	idle_ns := int64(0)
	defer func(t *int64) {
		//fmt.Printf("txbuilder idleness: %v s\n", float64(*t)/1000000000)
	}(&idle_ns)

	for {
		var event *BinlogEvent

		select {
		case event = <-tb.EvtChan:
		default:
			t1 := time.Now().UnixNano()
			event = <-tb.EvtChan
			t2 := time.Now().UnixNano()
			idle_ns += (t2 - t1)
		}
		if event.Err != nil {
			tb.Cfg.ErrCh <- event.Err
			return
		}

		if tb.currentTx != nil {
			tb.currentTx.eventCount++
			tb.currentTx.EventSize += uint64(event.Header.EventSize)
		}

		switch event.Header.EventType {
		case binlog.GTID_EVENT:
			if tb.currentTx != nil {
				tb.Cfg.ErrCh <- fmt.Errorf("unfinished transaction %v@%v", event.BinlogFile, event.RealPos)
				return
			}
			tb.newTransaction(event)

		case binlog.QUERY_EVENT:
			if tb.currentTx == nil {
				tb.Cfg.ErrCh <- newTxWithoutGTIDError(event)
				return
			}
			err := tb.onQueryEvent(event)
			if err != nil {
				tb.Cfg.ErrCh <- err
				return
			}

		case binlog.XID_EVENT:
			if tb.currentTx == nil {
				tb.Cfg.ErrCh <- newTxWithoutGTIDError(event)
				return
			}
			tb.onCommit(event)

		// process: optional FDE event -> TableMapEvent -> RowEvents
		case binlog.FORMAT_DESCRIPTION_EVENT:
			tb.currentFde = "BINLOG '\n" + base64.StdEncoding.EncodeToString(event.RawBs) + "\n'"

		case binlog.TABLE_MAP_EVENT:
			err := tb.onTableMapEvent(event)
			if err != nil {
				tb.Cfg.ErrCh <- err
				return
			}

		case binlog.WRITE_ROWS_EVENTv2, binlog.UPDATE_ROWS_EVENTv2, binlog.DELETE_ROWS_EVENTv2:
			err := tb.onRowEvent(event)
			if err != nil {
				tb.Cfg.ErrCh <- err
				return
			}
		}
	}
}

// event handlers
func (tb *TxBuilder) newTransaction(event *BinlogEvent) {
	evt := event.Evt.(*binlog.GTIDEvent)
	u, _ := uuid.FromBytes(evt.SID)

	tb.currentTx = &Transaction_t{
		ServerId: fmt.Sprintf("%d", event.Header.ServerID),
		SID:      u.String(),
		GNO:      evt.GNO,
		//Gtid:           fmt.Sprintf("%s:%d", u.String(), evt.GNO),
		StartEventFile: event.BinlogFile,
		StartEventPos:  event.RealPos,
		Impacting:      map[uint64]([]string){},
		EventSize:      uint64(event.Header.EventSize),
		Query:          []string{},
	}
}

func (tb *TxBuilder) onQueryEvent(event *BinlogEvent) error {
	evt := event.Evt.(*binlog.QueryEvent)
	query := string(evt.Query)

	// a tx should contain one DDL at most
	if tb.currentTx.ErrorCode != evt.ErrorCode {
		if tb.currentTx.ErrorCode != 0 {
			return fmt.Errorf("multiple error code in a tx. see txBuilder.onQueryEvent()")
		}
		tb.currentTx.ErrorCode = evt.ErrorCode
	}

	if strings.ToUpper(query) == "BEGIN" {
		tb.currentTx.hasBeginQuery = true
	} else {
		// DDL or statement/mixed binlog format
		tb.setImpactOnAll()
		if strings.ToUpper(query) == "COMMIT" || !tb.currentTx.hasBeginQuery {
			sqls, ok, err := usql.ResolveDDLSQL(query)
			if err != nil {
				return fmt.Errorf("parse query event failed: %v", err)
			}
			if !ok {
				tb.onCommit(event)
				return nil
			}

			for _, sql := range sqls {
				if tb.skipQueryDDL(sql, string(evt.Schema)) {
					ulog.Logger.WithFields(logrus.Fields{
						"schema": fmt.Sprintf("%s", evt.Schema),
						"sql":    fmt.Sprintf("%s", sql),
					}).Debug("builder: skip query-ddl-sql")
					continue
				}

				sql, err = usql.GenDDLSQL(sql, string(evt.Schema))
				if err != nil {
					return err
				}
				event.Query = append(event.Query, sql)
			}
			tb.onCommit(event)
		}
	}

	return nil
}

func (tb *TxBuilder) onTableMapEvent(event *BinlogEvent) error {
	if tb.currentTx.ImpactingAll {
		return nil
	}

	ev := event.Evt.(*binlog.TableMapEvent)
	if tb.skipRowEvent(string(ev.Schema), string(ev.Table)) {
		ulog.Logger.WithFields(logrus.Fields{
			"schema": fmt.Sprintf("%s", ev.Schema),
			"table":  fmt.Sprintf("%s", ev.Table),
		}).Debug("builder: skip TableMapEvent")
		return nil
	}

	tb.appendB64TableMapEvent(event)
	return nil
}

func (tb *TxBuilder) onRowEvent(event *BinlogEvent) error {
	if tb.currentTx.ImpactingAll {
		return nil
	}

	ev := event.Evt.(*binlog.RowsEvent)

	if tb.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table)) {
		ulog.Logger.WithFields(logrus.Fields{
			"schema": fmt.Sprintf("%s", ev.Table.Schema),
			"table":  fmt.Sprintf("%s", ev.Table.Table),
		}).Debug("builder: skip RowsEvent")
		return nil
	}

	tb.appendB64RowEvent(event)

	return nil
}

//
func (tb *TxBuilder) setImpactOnAll() {
	tb.currentTx.ImpactingAll = true
	tb.clearB64Sql()
}
func (tb *TxBuilder) clearB64Sql() {
	tb.arrayCurrentSqlB64 = nil
}

var appendB64SqlBs []byte = make([]byte, 1024*1024)

func (tb *TxBuilder) appendB64TableMapEvent(event *BinlogEvent) {
	tb.currentSqlB64 = new(bytes.Buffer)
	tb.currentSqlB64.WriteString("BINLOG '")
	n := base64.StdEncoding.EncodedLen(len(event.RawBs))
	// enlarge only
	if len(appendB64SqlBs) < n {
		appendB64SqlBs = make([]byte, n)
	}
	base64.StdEncoding.Encode(appendB64SqlBs, event.RawBs)
	tb.currentSqlB64.Write(appendB64SqlBs[0:n])

	tb.currentSqlB64.WriteString("\n")
}

func (tb *TxBuilder) appendB64RowEvent(event *BinlogEvent) {
	n := base64.StdEncoding.EncodedLen(len(event.RawBs))
	// enlarge only
	if len(appendB64SqlBs) < n {
		appendB64SqlBs = make([]byte, n)
	}
	base64.StdEncoding.Encode(appendB64SqlBs, event.RawBs)
	tb.currentSqlB64.Write(appendB64SqlBs[0:n])

	tb.currentSqlB64.WriteString("\n'")
	tb.arrayCurrentSqlB64 = append(tb.arrayCurrentSqlB64, tb.currentSqlB64.String())
	tb.currentSqlB64 = nil
}

func (tb *TxBuilder) onCommit(lastEvent *BinlogEvent) {
	tx := tb.currentTx
	if nil != tb.arrayCurrentSqlB64 {
		tx.Query = tb.arrayCurrentSqlB64
		tx.Fde = tb.currentFde
	} else {
		tx.Query = append(tx.Query, lastEvent.Query...)
	}

	tx.EndEventFile = lastEvent.BinlogFile
	tx.EndEventPos = lastEvent.RealPos

	tb.TxChan <- tb.currentTx

	tb.currentTx = nil
	tb.arrayCurrentSqlB64 = nil
}

func newTxWithoutGTIDError(event *BinlogEvent) error {
	return fmt.Errorf("transaction without GTID_EVENT %v@%v", event.BinlogFile, event.RealPos)
}

func (tb *TxBuilder) matchString(pattern string, t string) bool {
	if re, ok := tb.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (tb *TxBuilder) matchTable(patternTBS []uconf.TableName, t uconf.TableName) bool {
	for _, ptb := range patternTBS {
		retb, oktb := tb.reMap[ptb.Name]
		redb, okdb := tb.reMap[ptb.Schema]

		if oktb && okdb {
			if redb.MatchString(t.Schema) && retb.MatchString(t.Name) {
				return true
			}
		}
		if oktb {
			if retb.MatchString(t.Name) && t.Schema == ptb.Schema {
				return true
			}
		}
		if okdb {
			if redb.MatchString(t.Schema) && t.Name == ptb.Name {
				return true
			}
		}

		//create database or drop database
		if t.Name == "" {
			if t.Schema == ptb.Schema {
				return true
			}
		}

		if (ptb.Schema == t.Schema || ptb.Schema == "") && (ptb.Name == t.Name || ptb.Name == "") {
			return true
		}
	}

	return false
}

func (tb *TxBuilder) skipQueryDDL(sql string, schema string) bool {
	t, err := usql.ParserDDLTableName(sql)
	if err != nil {
		ulog.Logger.WithField("sql", sql).WithError(err).Error("builder: Get table failure")
		return false
	}

	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(tb.Cfg.ReplicateDoDb) > 0 {
			//if table in target Table, do this sql
			if t.Schema == "" {
				t.Schema = schema
			}
			if tb.matchTable(tb.Cfg.ReplicateDoDb, t) {
				return false
			}
			return true
		}
	}
	return false
}

func (tb *TxBuilder) skipRowEvent(schema string, table string) bool {
	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(tb.Cfg.ReplicateDoDb) > 0 {
			table = strings.ToLower(table)
			//if table in tartget Table, do this event
			for _, d := range tb.Cfg.ReplicateDoDb {
				if (tb.matchString(d.Schema, schema) || d.Schema == "") && (tb.matchString(d.Name, table) || d.Name == "") {
					return false
				}
			}
			return true
		}
	}
	return false
}
