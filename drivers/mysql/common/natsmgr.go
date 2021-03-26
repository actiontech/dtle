package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
)

type NatsMsgMerger struct {
	buf    *bytes.Buffer
	iSeg   uint32 // index of segment
	logger hclog.Logger
}

func NewNatsMsgMerger(logger hclog.Logger) *NatsMsgMerger {
	return &NatsMsgMerger{
		logger: logger,
	}
}
func (nmm *NatsMsgMerger) GetBuf() bytes.Buffer {
	return *nmm.buf
}
func (nmm *NatsMsgMerger) Handle(data []byte) (segmentFinished bool, err error) {
	// non-big msg: < NatsMaxMsg
	// non-last big msg segment: NatsMaxMsg + 4
	// last big msg segment: 4 ~ NatsMaxMsg + 3
	lenData := len(data)
	if nmm.iSeg == 0 {
		if lenData < g.NatsMaxMsg {
			nmm.logger.Debug("NatsMsgMerger.Handle found ordinary msg", "lenData", lenData)
			segmentFinished = true
			nmm.buf = bytes.NewBuffer(data)
		} else {
			segmentFinished = false
			iSeg := binary.LittleEndian.Uint32(data[lenData-4 : lenData])
			if iSeg != 0 {
				return false, fmt.Errorf("bad index for big msg segment. expect 0 got %v lenData %v ", iSeg, lenData)
			}
			nmm.logger.Debug("NatsMsgMerger.Handle found big msg segment", "iSeg", iSeg, "lenData", lenData)
			nmm.buf = bytes.NewBuffer(data[:lenData-4])
		}
		nmm.iSeg += 1
	} else {
		if lenData < 4 {
			return false, fmt.Errorf("lenData for big msg segment should not be less than 4. iSeg %v lenData %v ",
				nmm.iSeg, lenData)
		}
		iSeg := binary.LittleEndian.Uint32(data[lenData-4 : lenData])
		if iSeg != nmm.iSeg {
			if iSeg == nmm.iSeg-1 {
				nmm.logger.Debug("full. ignore resent segment", "expect", nmm.iSeg, "got", iSeg)
			} else {
				nmm.logger.Warn("DTLE_BUG: full. bad segment", "expect", nmm.iSeg, "got", iSeg)
			}
		} else {
			nmm.buf.Write(data[:lenData-4])
			nmm.iSeg += 1
		}

		if lenData < g.NatsMaxMsg+4 {
			segmentFinished = true
		} else {
			segmentFinished = false
		}
		nmm.logger.Debug("NatsMsgMerger.Handle found big msg segment", "iSeg", iSeg, "lenData", lenData,
			"isLast", segmentFinished)
	}
	return segmentFinished, nil
}

func (nmm *NatsMsgMerger) Reset() {
	nmm.logger.Debug("NatsMsgMerger.Reset", "iSeg", nmm.iSeg)

	nmm.buf = nil
	nmm.iSeg = 0
}
