package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"github.com/actiontech/dtle/helper/u"
	"github.com/actiontech/dtle/internal/client/driver/mysql/base"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

const (
	GTID_SUMMARY byte = 1
	GTID_SINGLE byte = 2
)
func main() {
	var err error
	intervals := gomysql.IntervalSlice{}
	var i2 []int64
	for i := int64(123456789); i <= 123456789 + 100000; i += 2 {
		intervals = append(intervals, gomysql.Interval{Start:i, Stop: i + 1})
		i2 = append(i2, i)
	}
	intervals = intervals.Normalize()
	s := base.StringInterval(intervals)
	fmt.Printf("len(s) = %v\n", len(s))

	fmt.Printf("intervals: %v\n", s[0:256])

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(GTID_SUMMARY)
	err = binary.Write(buf, binary.LittleEndian, intervals)
	u.PanicIfErr(err)

	gzBuf := new(bytes.Buffer)
	gzWriter := gzip.NewWriter(gzBuf)
	_, err = gzWriter.Write(buf.Bytes())
	u.PanicIfErr(err)
	gzWriter.Close()
	fmt.Printf("len(gzBuf) = %v\n", gzBuf.Len())

	gzBuf.Reset()
	gzWriter.Reset(gzBuf)
	_, err = gzWriter.Write([]byte(s))
	u.PanicIfErr(err)
	gzWriter.Close()
	fmt.Printf("len(gzBuf) = %v\n", gzBuf.Len())

	buf.Reset()
	for j := range i2 {
		err = binary.Write(buf, binary.LittleEndian, i2[j])
		u.PanicIfErr(err)
	}
	gzBuf.Reset()
	gzWriter.Reset(gzBuf)
	_, err = gzWriter.Write(buf.Bytes())
	u.PanicIfErr(err)
	gzWriter.Close()
	fmt.Printf("len(gzBuf) = %v\n", gzBuf.Len())
}
