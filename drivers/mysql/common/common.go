package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/actiontech/dtle/g"
	"github.com/golang/snappy"
	"github.com/pingcap/tidb/types"
)

func init() {
	gob.Register(types.BinaryLiteral{})
}

type ExecContext struct {
	Subject    string
	StateDir   string
}

func ValidateJobName(name string) error {
	if len(name) > g.JobNameLenLimit {
		return fmt.Errorf("job name too long. jobName %v lenLimit %v", name, g.JobNameLenLimit)
	}
	return nil
}

// Encode
func GobEncode(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func Encode(v interface{}) ([]byte, error) {
	b := new(bytes.Buffer)
	enc := gob.NewEncoder(b)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b.Bytes()), nil
}

// Decode
func Decode(data []byte, vPtr interface{}) (err error) {
	msg, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	return gob.NewDecoder(bytes.NewBuffer(msg)).Decode(vPtr)
}

func DecodeGob(data []byte, vPtr interface{}) (err error) {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(vPtr)
}

func DecodeDumpEntry(data []byte) (entry *DumpEntry, err error) {
	msg, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	entry = &DumpEntry{}
	n, err := entry.Unmarshal(msg)
	if err != nil {
		return nil, err
	}
	if n != uint64(len(msg)) {
		return nil, fmt.Errorf("DumpEntry.Unmarshal: not all consumed. data: %v, consumed: %v",
			len(msg), n)
	}
	return entry, nil
}

func (d *DumpEntry) IncrementCounter() {
	d.RowsCount++
}
