package binlog

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/issuj/gofaster/base64"
	"github.com/ngaut/log"
	"github.com/satori/go.uuid"
	binlog "github.com/siddontang/go-mysql/replication"

	uconf "udup/config"
)

type Transaction_t struct {
	StartEventFile string
	StartEventPos  uint32
	EndEventFile   string
	EndEventPos    uint32
	Gtid           string
	hasBeginQuery  bool
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
	cfg     *uconf.DriverConfig
	EvtChan chan *BinlogEvent
	TxChan  chan *Transaction_t

	currentTx          *Transaction_t
	txCount            int
	currentFde         string
	currentSqlB64      *bytes.Buffer
	arrayCurrentSqlB64 []string
}

func NewTxBuilder(cfg *uconf.DriverConfig) *TxBuilder {
	return &TxBuilder{
		cfg:     cfg,
		EvtChan: make(chan *BinlogEvent, 200),
		TxChan:  make(chan *Transaction_t, 100),
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
			tb.cfg.ErrCh <- event.Err
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
				tb.cfg.ErrCh <- fmt.Errorf("unfinished transaction %v@%v", event.BinlogFile, event.RealPos)
				return
			}
			tb.newTransaction(event)

		case binlog.QUERY_EVENT:
			if tb.currentTx == nil {
				tb.cfg.ErrCh <- newTxWithoutGTIDError(event)
				return
			}
			err := tb.onQueryEvent(event)
			if err != nil {
				tb.cfg.ErrCh <- err
				return
			}

		case binlog.XID_EVENT:
			if tb.currentTx == nil {
				tb.cfg.ErrCh <- newTxWithoutGTIDError(event)
				return
			}
			tb.onCommit(event)

		// process: optional FDE event -> TableMapEvent -> RowEvents
		case binlog.FORMAT_DESCRIPTION_EVENT:
			tb.currentFde = "BINLOG '\n" + base64.StdEncoding.EncodeToString(event.RawBs) + "\n'"

		case binlog.TABLE_MAP_EVENT:
			err := tb.onTableMapEvent(event)
			if err != nil {
				tb.cfg.ErrCh <- err
				return
			}

		case binlog.WRITE_ROWS_EVENTv2, binlog.UPDATE_ROWS_EVENTv2, binlog.DELETE_ROWS_EVENTv2:
			err := tb.onRowEvent(event)
			if err != nil {
				tb.cfg.ErrCh <- err
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
		Gtid:           fmt.Sprintf("%s:%d", u.String(), evt.GNO),
		StartEventFile: event.BinlogFile,
		StartEventPos:  event.RealPos,
		Impacting:      map[uint64]([]string){},
		EventSize:      uint64(event.Header.EventSize),
		Query:          []string{},
	}
}

func (tb *TxBuilder) onQueryEvent(event *BinlogEvent) error {
	evt := event.Evt.(*binlog.QueryEvent)

	// a tx should contain one DDL at most
	if tb.currentTx.ErrorCode != evt.ErrorCode {
		if tb.currentTx.ErrorCode != 0 {
			return fmt.Errorf("multiple error code in a tx. see txBuilder.onQueryEvent()")
		}
		tb.currentTx.ErrorCode = evt.ErrorCode
	}

	query := string(evt.Query)
	if strings.ToUpper(query) == "BEGIN" {
		tb.currentTx.hasBeginQuery = true
	} else {
		// DDL or statement/mixed binlog format
		tb.setImpactOnAll()
		if strings.ToUpper(query) == "COMMIT" || !tb.currentTx.hasBeginQuery {
			if tb.skipQueryEvent(query) {
				log.Infof("skip query %s", query)
				tb.onCommit(event)
				return nil
			}
			if strings.HasPrefix(strings.ToUpper(query), "CREATE DATABASE") || string(evt.Schema) == "" {
				event.Query = query
			} else {
				event.Query = fmt.Sprintf("USE %s; %s;", string(evt.Schema), query)
			}

			tb.onCommit(event)
		}
	}

	return nil
}

func (tb *TxBuilder) skipQueryEvent(sql string) bool {
	sql = strings.ToUpper(sql)

	if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE ON") {
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

func (tb *TxBuilder) onTableMapEvent(event *BinlogEvent) error {
	if tb.currentTx.ImpactingAll {
		return nil
	}

	/*evt := event.Evt.(*binlog.TableMapEvent)
	log.Infof("evt.TableID:%v,---evt.Schema:%v,---evt.Table:%v",evt.TableID, string(evt.Schema), string(evt.Table))

	tableInfo, err := tb.tc.queryTableInfo(evt.TableID, string(evt.Schema), string(evt.Table))
	if err != nil {
		return err
	}

	if tb.cfg.Evaling && tableInfo.hasNoPk() {
		//tb.booster.eval.TableWithoutPk[fmt.Sprintf("%v.%v", tableInfo.schema, tableInfo.name)] = true
	}*/

	tb.appendB64TableMapEvent(event)
	return nil
}

func (tb *TxBuilder) onRowEvent(event *BinlogEvent) error {
	if tb.currentTx.ImpactingAll {
		return nil
	}

	/*evt := event.Evt.(*binlog.RowsEvent)

	tableInfo, err := tb.tc.getTableInfo(evt.TableID)
	if nil != err {
		return err
	}

	if tableInfo.hasNoPk() {
		tb.setImpactOnAll()
		return nil
	}*/

	/*for _, row := range evt.Rows {
		rowId := tableInfo.getRowId(row)
		tb.currentTx.addImpact(evt.TableID, rowId)

		n_impacted := len(tb.currentTx.Impacting[evt.TableID])
		if n_impacted > tb.cfg.TxImpactLimit {
			tb.setImpactOnAll()
			return nil
		}
	}*/
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
		tx.Query = append(tx.Query, lastEvent.Query)
	}

	tx.EndEventFile = lastEvent.BinlogFile
	tx.EndEventPos = lastEvent.RealPos

	log.Debugf("TB -> %+v", tb.currentTx)
	if tb.cfg.Evaling {
		tb.txCount++
	}
	tb.TxChan <- tb.currentTx

	tb.currentTx = nil
	tb.arrayCurrentSqlB64 = nil
}

func newTxWithoutGTIDError(event *BinlogEvent) error {
	return fmt.Errorf("transaction without GTID_EVENT %v@%v", event.BinlogFile, event.RealPos)
}
