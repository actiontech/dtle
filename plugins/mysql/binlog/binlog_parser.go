package binlog

import (
	"fmt"
	"sync"

	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	uconf "udup/config"
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
	EventsChannel   chan *BinlogEvent
	lastPos         int64
	currentFilePath string
	currentHeader   *replication.EventHeader

	config                   *uconf.DriverConfig
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint BinlogCoordinates
}

func NewBinlogParser(config *uconf.DriverConfig) (binlogParser *BinlogParser, err error) {
	binlogParser = &BinlogParser{
		config:                  config,
		currentCoordinates:      BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(config.ServerID),
		Flavor:          "mysql",
		Host:            config.ConnCfg.Host,
		Port:            uint16(config.ConnCfg.Port),
		User:            config.ConnCfg.User,
		Password:        config.ConnCfg.Password,
		RawModeEanbled:  false,
		SemiSyncEnabled: false,
	}
	log.Infof("Registering replica at %+v:%+v", config.ConnCfg.Host, uint16(config.ConnCfg.Port))
	binlogParser.binlogSyncer = replication.NewBinlogSyncer(&cfg)
	return binlogParser, err
}

// ConnectBinlogStreamer
func (bp *BinlogParser) ConnectBinlogStreamer(coordinates BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return fmt.Errorf("Emptry coordinates at ConnectBinlogStreamer()")
	}

	bp.currentCoordinates = coordinates
	log.Infof("Connecting binlog streamer at %+v", bp.currentCoordinates)
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

func (bp *BinlogParser) StreamEvents(canStreaming func() bool, eventsChannel chan<- *BinlogEvent) error {
	if !canStreaming() {
		return nil
	}
	for {
		if !canStreaming() {
			break
		}
		ev, err := bp.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

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
			log.Infof("Rotate to next log name: %s", rotateEvent.NextLogName)
		} else {
			if err := bp.handleRowsEvent(ev, eventsChannel); err != nil {
				return err
			}
		}
	}
	log.Debugf("Done streaming events")

	return nil
}

func (bp *BinlogParser) handleRowsEvent(ev *replication.BinlogEvent, eventsChannel chan<- *BinlogEvent) error {
	if bp.currentCoordinates.SmallerThanOrEquals(&bp.LastAppliedRowsEventHint) {
		log.Debugf("Skipping handled query at %+v", bp.currentCoordinates)
		return nil
	}

	switch ev.Header.EventType {
	case replication.STOP_EVENT:
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
	bp.binlogSyncer.Close()
	return nil
}
