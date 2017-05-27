package binlog

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/siddontang/go-mysql/replication"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"golang.org/x/net/context"
	"github.com/satori/go.uuid"

	uutil "udup/internal/client/driver/mysql/util"
	uconf "udup/internal/config"
)

type BinlogEvent struct {
	BinlogFile string
	RealPos    uint32
	Header     *replication.EventHeader
	Evt        replication.Event
	RawBs      []byte
	Query      []string

	Err error
}

type BinlogParser struct {
	lastPos         int64
	currentFilePath string
	currentHeader   *replication.EventHeader

	logger                   *log.Logger
	config                   *uconf.MySQLDriverConfig
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint BinlogCoordinates
	WaitCh                   chan error
}

func NewBinlogParser(config *uconf.MySQLDriverConfig, logger *log.Logger) (binlogParser *BinlogParser, err error) {
	binlogParser = &BinlogParser{
		logger:                  logger,
		config:                  config,
		currentCoordinates:      BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
	}

	id, err := uutil.NewIdWorker(2, 3, uutil.SnsEpoch)
	if err != nil {
		return nil, err
	}
	sid, err := id.NextId()
	if err != nil {
		return nil, err
	}

	bid := []byte(strconv.FormatUint(uint64(sid), 10))
	uid, err := strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		return nil, err
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(uid),
		Flavor:          "mysql",
		Host:            config.Dsn.Host,
		Port:            uint16(config.Dsn.Port),
		User:            config.Dsn.User,
		Password:        config.Dsn.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}
	logger.Printf("[INFO] mysql.parser: registering replica at %+v:%+v with server-id %+v", config.Dsn.Host, uint16(config.Dsn.Port), uint32(uid))

	binlogParser.binlogSyncer = replication.NewBinlogSyncer(&cfg)
	return binlogParser, err
}

// ConnectBinlogStreamer
func (bp *BinlogParser) ConnectBinlogStreamer(coordinates BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return fmt.Errorf("emptry coordinates at ConnectBinlogStreamer()")
	}

	bp.currentCoordinates = coordinates
	bp.logger.Printf("[INFO] mysql.parser: connecting binlog streamer at %+v", bp.currentCoordinates)
	// Start sync with sepcified binlog file and position
	bp.binlogStreamer, err = bp.binlogSyncer.StartSyncGTID(bp.currentCoordinates.GtidSet)
	return err
}

func (bp *BinlogParser) GetCurrentBinlogCoordinates() *BinlogCoordinates {
	bp.currentCoordinatesMutex.Lock()
	defer bp.currentCoordinatesMutex.Unlock()
	returnCoordinates := bp.currentCoordinates
	return &returnCoordinates
}

func (bp *BinlogParser) StreamEvents(eventsChannel chan<- *BinlogEvent) error {
	for {
		ev, err := bp.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		//bp.logger.Printf("-----bp.currentCoordinates:%v",bp.currentCoordinates)
		//ev.Dump(os.Stdout)
		func() {
			bp.currentCoordinatesMutex.Lock()
			defer bp.currentCoordinatesMutex.Unlock()
			bp.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				bp.currentCoordinatesMutex.Lock()
				defer bp.currentCoordinatesMutex.Unlock()
				bp.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()
			bp.logger.Printf("[INFO] mysql.parser: rotate to next log name: %s", rotateEvent.NextLogName)
		} else {
			if err := bp.handleRowsEvent(ev, eventsChannel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bp *BinlogParser) handleRowsEvent(ev *replication.BinlogEvent, eventsChannel chan<- *BinlogEvent) error {
	//bp.logger.Printf("---c:%v----:%v",bp.currentCoordinates,bp.LastAppliedRowsEventHint)
	if bp.currentCoordinates.SmallerThanOrEquals(&bp.LastAppliedRowsEventHint) {
		bp.logger.Printf("[DEBUG] mysql.parser: skipping handled query at %+v", bp.currentCoordinates)
		return nil
	}

	switch ev.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}
	case replication.GTID_EVENT:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}
		if gtidEvent, ok := ev.Event.(*replication.GTIDEvent); ok {
			func() {
				bp.currentCoordinatesMutex.Lock()
				defer bp.currentCoordinatesMutex.Unlock()
				u, _ := uuid.FromBytes(gtidEvent.SID)
				gtidSet, err := gomysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d",u.String(),gtidEvent.GNO))
				if err != nil {
					bp.logger.Printf("[ERR]: parse mysql gtid error %v",err)
				}
				bp.currentCoordinates.GtidSet = gtidSet
			}()
		}
	case replication.QUERY_EVENT:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
		}
	case replication.TABLE_MAP_EVENT:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
			Evt:        ev.Event,
			RawBs:      ev.RawData,
		}
	case replication.XID_EVENT:
		eventsChannel <- &BinlogEvent{
			BinlogFile: bp.currentCoordinates.LogFile,
			Header:     ev.Header,
			RealPos:    uint32(ev.Header.LogPos) - ev.Header.EventSize,
		}
	default:
		//ignore
	}
	bp.LastAppliedRowsEventHint = bp.currentCoordinates
	return nil
}

func (bp *BinlogParser) Close() error {
	//bp.binlogSyncer.Close()
	return nil
}
