package binlog

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/issuj/gofaster/base64"
	"github.com/ngaut/log"
	"github.com/satori/go.uuid"
	binlog "github.com/siddontang/go-mysql/replication"

	uconf "udup/config"
	usql "udup/plugins/mysql/sql"
)

const (
	EventsChannelBufferSize       = 200
)

type Transaction_t struct {
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

		log.Debugf("TB et:%v<- %+v", event.Header.EventType, event)

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
		SID: u.String(),
		GNO: evt.GNO,
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
			if skipQueryEvent(query) {
				log.Debugf("[skip query-sql]%s  [schema]:%s", query, string(evt.Schema))
				tb.onCommit(event)
				return nil
			}
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
					log.Debugf("[skip query-ddl-sql]%s  [schema]:%s", sql, evt.Schema)
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
		log.Debugf("[skip TableMapEvent]db:%s table:%s", ev.Schema, ev.Table)
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
		log.Debugf("[skip RowsEvent]db:%s table:%s", ev.Table.Schema, ev.Table.Table)
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

	log.Debugf("TB -> %+v", tb.currentTx)
	/*if tb.cfg.Evaling {
		tb.txCount++
	}*/
	tb.TxChan <- tb.currentTx

	tb.currentTx = nil
	tb.arrayCurrentSqlB64 = nil
}

func newTxWithoutGTIDError(event *BinlogEvent) error {
	return fmt.Errorf("transaction without GTID_EVENT %v@%v", event.BinlogFile, event.RealPos)
}

func (tb *TxBuilder) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if tb.matchString(b, a) {
			return true
		}
	}
	return false
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

		if ptb == t {
			return true
		}
	}

	return false
}

func skipQueryEvent(sql string) bool {
	sql = strings.ToUpper(sql)

	/*if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE") {
		return true
	}

	if strings.HasPrefix(sql, "GRANT ALL PRIVILEGES ON") {
		return true
	}

	if strings.HasPrefix(sql, "ALTER USER") {
		return true
	}

	if strings.HasPrefix(sql, "CREATE USER") {
		return true
	}

	if strings.HasPrefix(sql, "GRANT") {
		return true
	}*/

	if strings.HasPrefix(sql, "BEGIN") {
		return true
	}

	if strings.HasPrefix(sql, "COMMIT") {
		return true
	}

	if strings.HasPrefix(sql, "FLUSH PRIVILEGES") {
		return true
	}

	return false
}

func (tb *TxBuilder) skipQueryDDL(sql string, schema string) bool {
	t, err := usql.ParserDDLTableName(sql)
	if err != nil {
		log.Warnf("[get table failure]:%s %s", sql, err)
		return false
	}

	switch strings.ToLower(schema) {
	case "sys", "mysql", "information_schema", "performance_schema":
		return true
	default:
		if len(tb.Cfg.ReplicateDoTable) > 0 || len(tb.Cfg.ReplicateDoDb) > 0 {
			//if table in target Table, do this sql
			if t.Schema == "" {
				t.Schema = schema
			}
			if tb.matchTable(tb.Cfg.ReplicateDoTable, t) {
				return false
			}

			// if  schema in target DB, do this sql
			if tb.matchDB(tb.Cfg.ReplicateDoDb, t.Schema) {
				return false
			}
			return true
		}
	}
	return false
}

func (tb *TxBuilder) skipRowEvent(schema string, table string) bool {
	switch strings.ToLower(schema) {
	case "sys","mysql","information_schema","performance_schema":
		return true
	default:
		if len(tb.Cfg.ReplicateDoTable) > 0 || len(tb.Cfg.ReplicateDoDb) > 0 {
			table = strings.ToLower(table)
			//if table in tartget Table, do this event
			for _, d := range tb.Cfg.ReplicateDoTable {
				if tb.matchString(d.Schema, schema) && tb.matchString(d.Name, table) {
					return false
				}
			}

			//if schema in target DB, do this event
			if tb.matchDB(tb.Cfg.ReplicateDoDb, schema) && len(tb.Cfg.ReplicateDoDb) > 0 {
				return false
			}

			return true
		}
	}
	return false
}
