package common

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type DumpEntry struct {
	SystemVariablesStatement string
	SqlMode                  string
	DbSQL                    string
	TableName                string
	TableSchema              string
	TbSQL                    []string
	ValuesX                  [][]*[]byte
	TotalCount               int64
	Err                      string
	Table                    []byte
}

func (d *DumpEntry) Size() (s uint64) {

	{
		l := uint64(len(d.SystemVariablesStatement))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.SqlMode))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.DbSQL))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.TableName))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.TableSchema))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.TbSQL))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.TbSQL {

			{
				l := uint64(len(d.TbSQL[k0]))

				{

					t := l
					for t >= 0x80 {
						t >>= 7
						s++
					}
					s++

				}
				s += l
			}

		}

	}
	{
		l := uint64(len(d.ValuesX))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.ValuesX {

			{
				l := uint64(len(d.ValuesX[k0]))

				{

					t := l
					for t >= 0x80 {
						t >>= 7
						s++
					}
					s++

				}

				for k1 := range d.ValuesX[k0] {

					{
						if d.ValuesX[k0][k1] != nil {

							{
								l := uint64(len((*d.ValuesX[k0][k1])))

								{

									t := l
									for t >= 0x80 {
										t >>= 7
										s++
									}
									s++

								}
								s += l
							}
							s += 0
						}
					}

					s += 1

				}

			}

		}

	}
	{
		l := uint64(len(d.Err))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Table))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 8
	return
}
func (d *DumpEntry) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.SystemVariablesStatement))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.SystemVariablesStatement)
		i += l
	}
	{
		l := uint64(len(d.SqlMode))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.SqlMode)
		i += l
	}
	{
		l := uint64(len(d.DbSQL))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.DbSQL)
		i += l
	}
	{
		l := uint64(len(d.TableName))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.TableName)
		i += l
	}
	{
		l := uint64(len(d.TableSchema))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.TableSchema)
		i += l
	}
	{
		l := uint64(len(d.TbSQL))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.TbSQL {

			{
				l := uint64(len(d.TbSQL[k0]))

				{

					t := uint64(l)

					for t >= 0x80 {
						buf[i+0] = byte(t) | 0x80
						t >>= 7
						i++
					}
					buf[i+0] = byte(t)
					i++

				}
				copy(buf[i+0:], d.TbSQL[k0])
				i += l
			}

		}
	}
	{
		l := uint64(len(d.ValuesX))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.ValuesX {

			{
				l := uint64(len(d.ValuesX[k0]))

				{

					t := uint64(l)

					for t >= 0x80 {
						buf[i+0] = byte(t) | 0x80
						t >>= 7
						i++
					}
					buf[i+0] = byte(t)
					i++

				}
				for k1 := range d.ValuesX[k0] {

					{
						if d.ValuesX[k0][k1] == nil {
							buf[i+0] = 0
						} else {
							buf[i+0] = 1

							{
								l := uint64(len((*d.ValuesX[k0][k1])))

								{

									t := uint64(l)

									for t >= 0x80 {
										buf[i+1] = byte(t) | 0x80
										t >>= 7
										i++
									}
									buf[i+1] = byte(t)
									i++

								}
								copy(buf[i+1:], (*d.ValuesX[k0][k1]))
								i += l
							}
							i += 0
						}
					}

					i += 1

				}
			}

		}
	}
	{

		buf[i+0+0] = byte(d.TotalCount >> 0)

		buf[i+1+0] = byte(d.TotalCount >> 8)

		buf[i+2+0] = byte(d.TotalCount >> 16)

		buf[i+3+0] = byte(d.TotalCount >> 24)

		buf[i+4+0] = byte(d.TotalCount >> 32)

		buf[i+5+0] = byte(d.TotalCount >> 40)

		buf[i+6+0] = byte(d.TotalCount >> 48)

		buf[i+7+0] = byte(d.TotalCount >> 56)

	}
	{
		l := uint64(len(d.Err))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+8] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+8] = byte(t)
			i++

		}
		copy(buf[i+8:], d.Err)
		i += l
	}
	{
		l := uint64(len(d.Table))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+8] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+8] = byte(t)
			i++

		}
		copy(buf[i+8:], d.Table)
		i += l
	}
	return buf[:i+8], nil
}

func (d *DumpEntry) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.SystemVariablesStatement = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.SqlMode = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.DbSQL = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.TableName = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.TableSchema = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.TbSQL)) >= l {
			d.TbSQL = d.TbSQL[:l]
		} else {
			d.TbSQL = make([]string, l)
		}
		for k0 := range d.TbSQL {

			{
				l := uint64(0)

				{

					bs := uint8(7)
					t := uint64(buf[i+0] & 0x7F)
					for buf[i+0]&0x80 == 0x80 {
						i++
						t |= uint64(buf[i+0]&0x7F) << bs
						bs += 7
					}
					i++

					l = t

				}
				d.TbSQL[k0] = string(buf[i+0 : i+0+l])
				i += l
			}

		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.ValuesX)) >= l {
			d.ValuesX = d.ValuesX[:l]
		} else {
			d.ValuesX = make([][]*[]byte, l)
		}
		for k0 := range d.ValuesX {

			{
				l := uint64(0)

				{

					bs := uint8(7)
					t := uint64(buf[i+0] & 0x7F)
					for buf[i+0]&0x80 == 0x80 {
						i++
						t |= uint64(buf[i+0]&0x7F) << bs
						bs += 7
					}
					i++

					l = t

				}
				if uint64(cap(d.ValuesX[k0])) >= l {
					d.ValuesX[k0] = d.ValuesX[k0][:l]
				} else {
					d.ValuesX[k0] = make([]*[]byte, l)
				}
				for k1 := range d.ValuesX[k0] {

					{
						if buf[i+0] == 1 {
							if d.ValuesX[k0][k1] == nil {
								d.ValuesX[k0][k1] = new([]byte)
							}

							{
								l := uint64(0)

								{

									bs := uint8(7)
									t := uint64(buf[i+1] & 0x7F)
									for buf[i+1]&0x80 == 0x80 {
										i++
										t |= uint64(buf[i+1]&0x7F) << bs
										bs += 7
									}
									i++

									l = t

								}
								if uint64(cap((*d.ValuesX[k0][k1]))) >= l {
									(*d.ValuesX[k0][k1]) = (*d.ValuesX[k0][k1])[:l]
								} else {
									(*d.ValuesX[k0][k1]) = make([]byte, l)
								}
								copy((*d.ValuesX[k0][k1]), buf[i+1:])
								i += l
							}
							i += 0
						} else {
							d.ValuesX[k0][k1] = nil
						}
					}

					i += 1

				}
			}

		}
	}
	{

		d.TotalCount = 0 | (int64(buf[i+0+0]) << 0) | (int64(buf[i+1+0]) << 8) | (int64(buf[i+2+0]) << 16) | (int64(buf[i+3+0]) << 24) | (int64(buf[i+4+0]) << 32) | (int64(buf[i+5+0]) << 40) | (int64(buf[i+6+0]) << 48) | (int64(buf[i+7+0]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+8] & 0x7F)
			for buf[i+8]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+8]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.Err = string(buf[i+8 : i+8+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+8] & 0x7F)
			for buf[i+8]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+8]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Table)) >= l {
			d.Table = d.Table[:l]
		} else {
			d.Table = make([]byte, l)
		}
		copy(d.Table, buf[i+8:])
		i += l
	}
	return i + 8, nil
}

type MySQLCoordinateTx struct {
	LogFile       string
	LogPos        int64
	OSID          string
	SID           [16]byte
	GNO           int64
	LastCommitted int64
	SeqenceNumber int64
}

func (d *MySQLCoordinateTx) Size() (s uint64) {

	{
		l := uint64(len(d.LogFile))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.OSID))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		s += 16
	}
	s += 32
	return
}
func (d *MySQLCoordinateTx) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.LogFile))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.LogFile)
		i += l
	}
	{

		buf[i+0+0] = byte(d.LogPos >> 0)

		buf[i+1+0] = byte(d.LogPos >> 8)

		buf[i+2+0] = byte(d.LogPos >> 16)

		buf[i+3+0] = byte(d.LogPos >> 24)

		buf[i+4+0] = byte(d.LogPos >> 32)

		buf[i+5+0] = byte(d.LogPos >> 40)

		buf[i+6+0] = byte(d.LogPos >> 48)

		buf[i+7+0] = byte(d.LogPos >> 56)

	}
	{
		l := uint64(len(d.OSID))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+8] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+8] = byte(t)
			i++

		}
		copy(buf[i+8:], d.OSID)
		i += l
	}
	{
		copy(buf[i+8:], d.SID[:])
		i += 16
	}
	{

		buf[i+0+8] = byte(d.GNO >> 0)

		buf[i+1+8] = byte(d.GNO >> 8)

		buf[i+2+8] = byte(d.GNO >> 16)

		buf[i+3+8] = byte(d.GNO >> 24)

		buf[i+4+8] = byte(d.GNO >> 32)

		buf[i+5+8] = byte(d.GNO >> 40)

		buf[i+6+8] = byte(d.GNO >> 48)

		buf[i+7+8] = byte(d.GNO >> 56)

	}
	{

		buf[i+0+16] = byte(d.LastCommitted >> 0)

		buf[i+1+16] = byte(d.LastCommitted >> 8)

		buf[i+2+16] = byte(d.LastCommitted >> 16)

		buf[i+3+16] = byte(d.LastCommitted >> 24)

		buf[i+4+16] = byte(d.LastCommitted >> 32)

		buf[i+5+16] = byte(d.LastCommitted >> 40)

		buf[i+6+16] = byte(d.LastCommitted >> 48)

		buf[i+7+16] = byte(d.LastCommitted >> 56)

	}
	{

		buf[i+0+24] = byte(d.SeqenceNumber >> 0)

		buf[i+1+24] = byte(d.SeqenceNumber >> 8)

		buf[i+2+24] = byte(d.SeqenceNumber >> 16)

		buf[i+3+24] = byte(d.SeqenceNumber >> 24)

		buf[i+4+24] = byte(d.SeqenceNumber >> 32)

		buf[i+5+24] = byte(d.SeqenceNumber >> 40)

		buf[i+6+24] = byte(d.SeqenceNumber >> 48)

		buf[i+7+24] = byte(d.SeqenceNumber >> 56)

	}
	return buf[:i+32], nil
}

func (d *MySQLCoordinateTx) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.LogFile = string(buf[i+0 : i+0+l])
		i += l
	}
	{

		d.LogPos = 0 | (int64(buf[i+0+0]) << 0) | (int64(buf[i+1+0]) << 8) | (int64(buf[i+2+0]) << 16) | (int64(buf[i+3+0]) << 24) | (int64(buf[i+4+0]) << 32) | (int64(buf[i+5+0]) << 40) | (int64(buf[i+6+0]) << 48) | (int64(buf[i+7+0]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+8] & 0x7F)
			for buf[i+8]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+8]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.OSID = string(buf[i+8 : i+8+l])
		i += l
	}
	{
		copy(d.SID[:], buf[i+8:])
		i += 16
	}
	{

		d.GNO = 0 | (int64(buf[i+0+8]) << 0) | (int64(buf[i+1+8]) << 8) | (int64(buf[i+2+8]) << 16) | (int64(buf[i+3+8]) << 24) | (int64(buf[i+4+8]) << 32) | (int64(buf[i+5+8]) << 40) | (int64(buf[i+6+8]) << 48) | (int64(buf[i+7+8]) << 56)

	}
	{

		d.LastCommitted = 0 | (int64(buf[i+0+16]) << 0) | (int64(buf[i+1+16]) << 8) | (int64(buf[i+2+16]) << 16) | (int64(buf[i+3+16]) << 24) | (int64(buf[i+4+16]) << 32) | (int64(buf[i+5+16]) << 40) | (int64(buf[i+6+16]) << 48) | (int64(buf[i+7+16]) << 56)

	}
	{

		d.SeqenceNumber = 0 | (int64(buf[i+0+24]) << 0) | (int64(buf[i+1+24]) << 8) | (int64(buf[i+2+24]) << 16) | (int64(buf[i+3+24]) << 24) | (int64(buf[i+4+24]) << 32) | (int64(buf[i+5+24]) << 40) | (int64(buf[i+6+24]) << 48) | (int64(buf[i+7+24]) << 56)

	}
	return i + 32, nil
}

type OracleCoordinateTx struct {
	OldestUncommittedScn int64
	EndSCN               int64
}

func (d *OracleCoordinateTx) Size() (s uint64) {

	s += 16
	return
}
func (d *OracleCoordinateTx) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{

		buf[0+0] = byte(d.OldestUncommittedScn >> 0)

		buf[1+0] = byte(d.OldestUncommittedScn >> 8)

		buf[2+0] = byte(d.OldestUncommittedScn >> 16)

		buf[3+0] = byte(d.OldestUncommittedScn >> 24)

		buf[4+0] = byte(d.OldestUncommittedScn >> 32)

		buf[5+0] = byte(d.OldestUncommittedScn >> 40)

		buf[6+0] = byte(d.OldestUncommittedScn >> 48)

		buf[7+0] = byte(d.OldestUncommittedScn >> 56)

	}
	{

		buf[0+8] = byte(d.EndSCN >> 0)

		buf[1+8] = byte(d.EndSCN >> 8)

		buf[2+8] = byte(d.EndSCN >> 16)

		buf[3+8] = byte(d.EndSCN >> 24)

		buf[4+8] = byte(d.EndSCN >> 32)

		buf[5+8] = byte(d.EndSCN >> 40)

		buf[6+8] = byte(d.EndSCN >> 48)

		buf[7+8] = byte(d.EndSCN >> 56)

	}
	return buf[:i+16], nil
}

func (d *OracleCoordinateTx) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.OldestUncommittedScn = 0 | (int64(buf[0+0]) << 0) | (int64(buf[1+0]) << 8) | (int64(buf[2+0]) << 16) | (int64(buf[3+0]) << 24) | (int64(buf[4+0]) << 32) | (int64(buf[5+0]) << 40) | (int64(buf[6+0]) << 48) | (int64(buf[7+0]) << 56)

	}
	{

		d.EndSCN = 0 | (int64(buf[0+8]) << 0) | (int64(buf[1+8]) << 8) | (int64(buf[2+8]) << 16) | (int64(buf[3+8]) << 24) | (int64(buf[4+8]) << 32) | (int64(buf[5+8]) << 40) | (int64(buf[6+8]) << 48) | (int64(buf[7+8]) << 56)

	}
	return i + 16, nil
}

type BinlogCoordinatesX struct {
	LogFile string
	LogPos  int64
	GtidSet string
}

func (d *BinlogCoordinatesX) Size() (s uint64) {

	{
		l := uint64(len(d.LogFile))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.GtidSet))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 8
	return
}
func (d *BinlogCoordinatesX) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.LogFile))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.LogFile)
		i += l
	}
	{

		buf[i+0+0] = byte(d.LogPos >> 0)

		buf[i+1+0] = byte(d.LogPos >> 8)

		buf[i+2+0] = byte(d.LogPos >> 16)

		buf[i+3+0] = byte(d.LogPos >> 24)

		buf[i+4+0] = byte(d.LogPos >> 32)

		buf[i+5+0] = byte(d.LogPos >> 40)

		buf[i+6+0] = byte(d.LogPos >> 48)

		buf[i+7+0] = byte(d.LogPos >> 56)

	}
	{
		l := uint64(len(d.GtidSet))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+8] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+8] = byte(t)
			i++

		}
		copy(buf[i+8:], d.GtidSet)
		i += l
	}
	return buf[:i+8], nil
}

func (d *BinlogCoordinatesX) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.LogFile = string(buf[i+0 : i+0+l])
		i += l
	}
	{

		d.LogPos = 0 | (int64(buf[i+0+0]) << 0) | (int64(buf[i+1+0]) << 8) | (int64(buf[i+2+0]) << 16) | (int64(buf[i+3+0]) << 24) | (int64(buf[i+4+0]) << 32) | (int64(buf[i+5+0]) << 40) | (int64(buf[i+6+0]) << 48) | (int64(buf[i+7+0]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+8] & 0x7F)
			for buf[i+8]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+8]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.GtidSet = string(buf[i+8 : i+8+l])
		i += l
	}
	return i + 8, nil
}

type DumpStatResult struct {
	Coord *BinlogCoordinatesX
	Type  int32
}

func (d *DumpStatResult) Size() (s uint64) {

	{
		if d.Coord != nil {

			{
				s += (*d.Coord).Size()
			}
			s += 0
		}
	}
	s += 5
	return
}
func (d *DumpStatResult) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		if d.Coord == nil {
			buf[i+0] = 0
		} else {
			buf[i+0] = 1

			{
				nbuf, err := (*d.Coord).Marshal(buf[1:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}
			i += 0
		}
	}
	{

		buf[i+0+1] = byte(d.Type >> 0)

		buf[i+1+1] = byte(d.Type >> 8)

		buf[i+2+1] = byte(d.Type >> 16)

		buf[i+3+1] = byte(d.Type >> 24)

	}
	return buf[:i+5], nil
}

func (d *DumpStatResult) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		if buf[i+0] == 1 {
			if d.Coord == nil {
				d.Coord = new(BinlogCoordinatesX)
			}

			{
				ni, err := (*d.Coord).Unmarshal(buf[i+1:])
				if err != nil {
					return 0, err
				}
				i += ni
			}
			i += 0
		} else {
			d.Coord = nil
		}
	}
	{

		d.Type = 0 | (int32(buf[i+0+1]) << 0) | (int32(buf[i+1+1]) << 8) | (int32(buf[i+2+1]) << 16) | (int32(buf[i+3+1]) << 24)

	}
	return i + 5, nil
}

type DataEvent struct {
	Query         string
	CurrentSchema string
	DatabaseName  string
	TableName     string
	DML           int8
	ColumnCount   uint64
	Table         []byte
	LogPos        int64
	Timestamp     uint32
	Flags         []byte
	FKParent      bool
	Rows          [][]interface{}
	DtleFlags     uint32
}

func (d *DataEvent) Size() (s uint64) {

	{
		l := uint64(len(d.Query))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.CurrentSchema))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.DatabaseName))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.TableName))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Table))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Flags))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Rows))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Rows {

			{
				l := uint64(len(d.Rows[k0]))

				{

					t := l
					for t >= 0x80 {
						t >>= 7
						s++
					}
					s++

				}

				for k1 := range d.Rows[k0] {

					{
						var v uint64
						switch d.Rows[k0][k1].(type) {

						case string:
							v = 0 + 1

						case int8:
							v = 1 + 1

						case int16:
							v = 2 + 1

						case int32:
							v = 3 + 1

						case int64:
							v = 4 + 1

						case uint8:
							v = 5 + 1

						case uint16:
							v = 6 + 1

						case uint32:
							v = 7 + 1

						case uint64:
							v = 8 + 1

						case []byte:
							v = 9 + 1

						case float32:
							v = 10 + 1

						case float64:
							v = 11 + 1

						case bool:
							v = 12 + 1

						}

						{

							t := v
							for t >= 0x80 {
								t >>= 7
								s++
							}
							s++

						}
						switch tt := d.Rows[k0][k1].(type) {

						case string:

							{
								l := uint64(len(tt))

								{

									t := l
									for t >= 0x80 {
										t >>= 7
										s++
									}
									s++

								}
								s += l
							}

						case int8:

							s += 1

						case int16:

							s += 2

						case int32:

							s += 4

						case int64:

							s += 8

						case uint8:

							s += 1

						case uint16:

							s += 2

						case uint32:

							s += 4

						case uint64:

							s += 8

						case []byte:

							{
								l := uint64(len(tt))

								{

									t := l
									for t >= 0x80 {
										t >>= 7
										s++
									}
									s++

								}
								s += l
							}

						case float32:

							s += 4

						case float64:

							s += 8

						case bool:

							s += 1

						}
					}

				}

			}

		}

	}
	s += 26
	return
}
func (d *DataEvent) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.Query))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.Query)
		i += l
	}
	{
		l := uint64(len(d.CurrentSchema))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.CurrentSchema)
		i += l
	}
	{
		l := uint64(len(d.DatabaseName))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.DatabaseName)
		i += l
	}
	{
		l := uint64(len(d.TableName))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.TableName)
		i += l
	}
	{

		buf[i+0+0] = byte(d.DML >> 0)

	}
	{

		buf[i+0+1] = byte(d.ColumnCount >> 0)

		buf[i+1+1] = byte(d.ColumnCount >> 8)

		buf[i+2+1] = byte(d.ColumnCount >> 16)

		buf[i+3+1] = byte(d.ColumnCount >> 24)

		buf[i+4+1] = byte(d.ColumnCount >> 32)

		buf[i+5+1] = byte(d.ColumnCount >> 40)

		buf[i+6+1] = byte(d.ColumnCount >> 48)

		buf[i+7+1] = byte(d.ColumnCount >> 56)

	}
	{
		l := uint64(len(d.Table))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+9] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+9] = byte(t)
			i++

		}
		copy(buf[i+9:], d.Table)
		i += l
	}
	{

		buf[i+0+9] = byte(d.LogPos >> 0)

		buf[i+1+9] = byte(d.LogPos >> 8)

		buf[i+2+9] = byte(d.LogPos >> 16)

		buf[i+3+9] = byte(d.LogPos >> 24)

		buf[i+4+9] = byte(d.LogPos >> 32)

		buf[i+5+9] = byte(d.LogPos >> 40)

		buf[i+6+9] = byte(d.LogPos >> 48)

		buf[i+7+9] = byte(d.LogPos >> 56)

	}
	{

		buf[i+0+17] = byte(d.Timestamp >> 0)

		buf[i+1+17] = byte(d.Timestamp >> 8)

		buf[i+2+17] = byte(d.Timestamp >> 16)

		buf[i+3+17] = byte(d.Timestamp >> 24)

	}
	{
		l := uint64(len(d.Flags))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+21] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+21] = byte(t)
			i++

		}
		copy(buf[i+21:], d.Flags)
		i += l
	}
	{
		if d.FKParent {
			buf[i+21] = 1
		} else {
			buf[i+21] = 0
		}
	}
	{
		l := uint64(len(d.Rows))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+22] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+22] = byte(t)
			i++

		}
		for k0 := range d.Rows {

			{
				l := uint64(len(d.Rows[k0]))

				{

					t := uint64(l)

					for t >= 0x80 {
						buf[i+22] = byte(t) | 0x80
						t >>= 7
						i++
					}
					buf[i+22] = byte(t)
					i++

				}
				for k1 := range d.Rows[k0] {

					{
						var v uint64
						switch d.Rows[k0][k1].(type) {

						case string:
							v = 0 + 1

						case int8:
							v = 1 + 1

						case int16:
							v = 2 + 1

						case int32:
							v = 3 + 1

						case int64:
							v = 4 + 1

						case uint8:
							v = 5 + 1

						case uint16:
							v = 6 + 1

						case uint32:
							v = 7 + 1

						case uint64:
							v = 8 + 1

						case []byte:
							v = 9 + 1

						case float32:
							v = 10 + 1

						case float64:
							v = 11 + 1

						case bool:
							v = 12 + 1

						}

						{

							t := uint64(v)

							for t >= 0x80 {
								buf[i+22] = byte(t) | 0x80
								t >>= 7
								i++
							}
							buf[i+22] = byte(t)
							i++

						}
						switch tt := d.Rows[k0][k1].(type) {

						case string:

							{
								l := uint64(len(tt))

								{

									t := uint64(l)

									for t >= 0x80 {
										buf[i+22] = byte(t) | 0x80
										t >>= 7
										i++
									}
									buf[i+22] = byte(t)
									i++

								}
								copy(buf[i+22:], tt)
								i += l
							}

						case int8:

							{

								buf[i+0+22] = byte(tt >> 0)

							}

							i += 1

						case int16:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

							}

							i += 2

						case int32:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

								buf[i+2+22] = byte(tt >> 16)

								buf[i+3+22] = byte(tt >> 24)

							}

							i += 4

						case int64:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

								buf[i+2+22] = byte(tt >> 16)

								buf[i+3+22] = byte(tt >> 24)

								buf[i+4+22] = byte(tt >> 32)

								buf[i+5+22] = byte(tt >> 40)

								buf[i+6+22] = byte(tt >> 48)

								buf[i+7+22] = byte(tt >> 56)

							}

							i += 8

						case uint8:

							{

								buf[i+0+22] = byte(tt >> 0)

							}

							i += 1

						case uint16:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

							}

							i += 2

						case uint32:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

								buf[i+2+22] = byte(tt >> 16)

								buf[i+3+22] = byte(tt >> 24)

							}

							i += 4

						case uint64:

							{

								buf[i+0+22] = byte(tt >> 0)

								buf[i+1+22] = byte(tt >> 8)

								buf[i+2+22] = byte(tt >> 16)

								buf[i+3+22] = byte(tt >> 24)

								buf[i+4+22] = byte(tt >> 32)

								buf[i+5+22] = byte(tt >> 40)

								buf[i+6+22] = byte(tt >> 48)

								buf[i+7+22] = byte(tt >> 56)

							}

							i += 8

						case []byte:

							{
								l := uint64(len(tt))

								{

									t := uint64(l)

									for t >= 0x80 {
										buf[i+22] = byte(t) | 0x80
										t >>= 7
										i++
									}
									buf[i+22] = byte(t)
									i++

								}
								copy(buf[i+22:], tt)
								i += l
							}

						case float32:

							{

								v := *(*uint32)(unsafe.Pointer(&(tt)))

								buf[i+0+22] = byte(v >> 0)

								buf[i+1+22] = byte(v >> 8)

								buf[i+2+22] = byte(v >> 16)

								buf[i+3+22] = byte(v >> 24)

							}

							i += 4

						case float64:

							{

								v := *(*uint64)(unsafe.Pointer(&(tt)))

								buf[i+0+22] = byte(v >> 0)

								buf[i+1+22] = byte(v >> 8)

								buf[i+2+22] = byte(v >> 16)

								buf[i+3+22] = byte(v >> 24)

								buf[i+4+22] = byte(v >> 32)

								buf[i+5+22] = byte(v >> 40)

								buf[i+6+22] = byte(v >> 48)

								buf[i+7+22] = byte(v >> 56)

							}

							i += 8

						case bool:

							{
								if tt {
									buf[i+22] = 1
								} else {
									buf[i+22] = 0
								}
							}

							i += 1

						}
					}

				}
			}

		}
	}
	{

		buf[i+0+22] = byte(d.DtleFlags >> 0)

		buf[i+1+22] = byte(d.DtleFlags >> 8)

		buf[i+2+22] = byte(d.DtleFlags >> 16)

		buf[i+3+22] = byte(d.DtleFlags >> 24)

	}
	return buf[:i+26], nil
}

func (d *DataEvent) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.Query = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.CurrentSchema = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.DatabaseName = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.TableName = string(buf[i+0 : i+0+l])
		i += l
	}
	{

		d.DML = 0 | (int8(buf[i+0+0]) << 0)

	}
	{

		d.ColumnCount = 0 | (uint64(buf[i+0+1]) << 0) | (uint64(buf[i+1+1]) << 8) | (uint64(buf[i+2+1]) << 16) | (uint64(buf[i+3+1]) << 24) | (uint64(buf[i+4+1]) << 32) | (uint64(buf[i+5+1]) << 40) | (uint64(buf[i+6+1]) << 48) | (uint64(buf[i+7+1]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+9] & 0x7F)
			for buf[i+9]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+9]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Table)) >= l {
			d.Table = d.Table[:l]
		} else {
			d.Table = make([]byte, l)
		}
		copy(d.Table, buf[i+9:])
		i += l
	}
	{

		d.LogPos = 0 | (int64(buf[i+0+9]) << 0) | (int64(buf[i+1+9]) << 8) | (int64(buf[i+2+9]) << 16) | (int64(buf[i+3+9]) << 24) | (int64(buf[i+4+9]) << 32) | (int64(buf[i+5+9]) << 40) | (int64(buf[i+6+9]) << 48) | (int64(buf[i+7+9]) << 56)

	}
	{

		d.Timestamp = 0 | (uint32(buf[i+0+17]) << 0) | (uint32(buf[i+1+17]) << 8) | (uint32(buf[i+2+17]) << 16) | (uint32(buf[i+3+17]) << 24)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+21] & 0x7F)
			for buf[i+21]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+21]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Flags)) >= l {
			d.Flags = d.Flags[:l]
		} else {
			d.Flags = make([]byte, l)
		}
		copy(d.Flags, buf[i+21:])
		i += l
	}
	{
		d.FKParent = buf[i+21] == 1
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+22] & 0x7F)
			for buf[i+22]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+22]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Rows)) >= l {
			d.Rows = d.Rows[:l]
		} else {
			d.Rows = make([][]interface{}, l)
		}
		for k0 := range d.Rows {

			{
				l := uint64(0)

				{

					bs := uint8(7)
					t := uint64(buf[i+22] & 0x7F)
					for buf[i+22]&0x80 == 0x80 {
						i++
						t |= uint64(buf[i+22]&0x7F) << bs
						bs += 7
					}
					i++

					l = t

				}
				if uint64(cap(d.Rows[k0])) >= l {
					d.Rows[k0] = d.Rows[k0][:l]
				} else {
					d.Rows[k0] = make([]interface{}, l)
				}
				for k1 := range d.Rows[k0] {

					{
						v := uint64(0)

						{

							bs := uint8(7)
							t := uint64(buf[i+22] & 0x7F)
							for buf[i+22]&0x80 == 0x80 {
								i++
								t |= uint64(buf[i+22]&0x7F) << bs
								bs += 7
							}
							i++

							v = t

						}
						switch v {

						case 0 + 1:
							var tt string

							{
								l := uint64(0)

								{

									bs := uint8(7)
									t := uint64(buf[i+22] & 0x7F)
									for buf[i+22]&0x80 == 0x80 {
										i++
										t |= uint64(buf[i+22]&0x7F) << bs
										bs += 7
									}
									i++

									l = t

								}
								tt = string(buf[i+22 : i+22+l])
								i += l
							}

							d.Rows[k0][k1] = tt

						case 1 + 1:
							var tt int8

							{

								tt = 0 | (int8(buf[i+0+22]) << 0)

							}

							i += 1

							d.Rows[k0][k1] = tt

						case 2 + 1:
							var tt int16

							{

								tt = 0 | (int16(buf[i+0+22]) << 0) | (int16(buf[i+1+22]) << 8)

							}

							i += 2

							d.Rows[k0][k1] = tt

						case 3 + 1:
							var tt int32

							{

								tt = 0 | (int32(buf[i+0+22]) << 0) | (int32(buf[i+1+22]) << 8) | (int32(buf[i+2+22]) << 16) | (int32(buf[i+3+22]) << 24)

							}

							i += 4

							d.Rows[k0][k1] = tt

						case 4 + 1:
							var tt int64

							{

								tt = 0 | (int64(buf[i+0+22]) << 0) | (int64(buf[i+1+22]) << 8) | (int64(buf[i+2+22]) << 16) | (int64(buf[i+3+22]) << 24) | (int64(buf[i+4+22]) << 32) | (int64(buf[i+5+22]) << 40) | (int64(buf[i+6+22]) << 48) | (int64(buf[i+7+22]) << 56)

							}

							i += 8

							d.Rows[k0][k1] = tt

						case 5 + 1:
							var tt uint8

							{

								tt = 0 | (uint8(buf[i+0+22]) << 0)

							}

							i += 1

							d.Rows[k0][k1] = tt

						case 6 + 1:
							var tt uint16

							{

								tt = 0 | (uint16(buf[i+0+22]) << 0) | (uint16(buf[i+1+22]) << 8)

							}

							i += 2

							d.Rows[k0][k1] = tt

						case 7 + 1:
							var tt uint32

							{

								tt = 0 | (uint32(buf[i+0+22]) << 0) | (uint32(buf[i+1+22]) << 8) | (uint32(buf[i+2+22]) << 16) | (uint32(buf[i+3+22]) << 24)

							}

							i += 4

							d.Rows[k0][k1] = tt

						case 8 + 1:
							var tt uint64

							{

								tt = 0 | (uint64(buf[i+0+22]) << 0) | (uint64(buf[i+1+22]) << 8) | (uint64(buf[i+2+22]) << 16) | (uint64(buf[i+3+22]) << 24) | (uint64(buf[i+4+22]) << 32) | (uint64(buf[i+5+22]) << 40) | (uint64(buf[i+6+22]) << 48) | (uint64(buf[i+7+22]) << 56)

							}

							i += 8

							d.Rows[k0][k1] = tt

						case 9 + 1:
							var tt []byte

							{
								l := uint64(0)

								{

									bs := uint8(7)
									t := uint64(buf[i+22] & 0x7F)
									for buf[i+22]&0x80 == 0x80 {
										i++
										t |= uint64(buf[i+22]&0x7F) << bs
										bs += 7
									}
									i++

									l = t

								}
								if uint64(cap(tt)) >= l {
									tt = tt[:l]
								} else {
									tt = make([]byte, l)
								}
								copy(tt, buf[i+22:])
								i += l
							}

							d.Rows[k0][k1] = tt

						case 10 + 1:
							var tt float32

							{

								v := 0 | (uint32(buf[i+0+22]) << 0) | (uint32(buf[i+1+22]) << 8) | (uint32(buf[i+2+22]) << 16) | (uint32(buf[i+3+22]) << 24)
								tt = *(*float32)(unsafe.Pointer(&v))

							}

							i += 4

							d.Rows[k0][k1] = tt

						case 11 + 1:
							var tt float64

							{

								v := 0 | (uint64(buf[i+0+22]) << 0) | (uint64(buf[i+1+22]) << 8) | (uint64(buf[i+2+22]) << 16) | (uint64(buf[i+3+22]) << 24) | (uint64(buf[i+4+22]) << 32) | (uint64(buf[i+5+22]) << 40) | (uint64(buf[i+6+22]) << 48) | (uint64(buf[i+7+22]) << 56)
								tt = *(*float64)(unsafe.Pointer(&v))

							}

							i += 8

							d.Rows[k0][k1] = tt

						case 12 + 1:
							var tt bool

							{
								tt = buf[i+22] == 1
							}

							i += 1

							d.Rows[k0][k1] = tt

						default:
							d.Rows[k0][k1] = nil
						}
					}

				}
			}

		}
	}
	{

		d.DtleFlags = 0 | (uint32(buf[i+0+22]) << 0) | (uint32(buf[i+1+22]) << 8) | (uint32(buf[i+2+22]) << 16) | (uint32(buf[i+3+22]) << 24)

	}
	return i + 26, nil
}

type BinlogEntry struct {
	Coordinates CoordinatesI
	Events      []DataEvent
	Index       int32
	Final       bool
}

func (d *BinlogEntry) Size() (s uint64) {

	{
		var v uint64
		switch d.Coordinates.(type) {

		case *MySQLCoordinateTx:
			v = 0 + 1

		case *OracleCoordinateTx:
			v = 1 + 1

		}

		{

			t := v
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		switch tt := d.Coordinates.(type) {

		case *MySQLCoordinateTx:

			{
				if tt != nil {

					{
						s += (*tt).Size()
					}
					s += 0
				}
			}

			s += 1

		case *OracleCoordinateTx:

			{
				if tt != nil {

					{
						s += (*tt).Size()
					}
					s += 0
				}
			}

			s += 1

		}
	}
	{
		l := uint64(len(d.Events))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Events {

			{
				s += d.Events[k0].Size()
			}

		}

	}
	s += 5
	return
}
func (d *BinlogEntry) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		var v uint64
		switch d.Coordinates.(type) {

		case *MySQLCoordinateTx:
			v = 0 + 1

		case *OracleCoordinateTx:
			v = 1 + 1

		}

		{

			t := uint64(v)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		switch tt := d.Coordinates.(type) {

		case *MySQLCoordinateTx:

			{
				if tt == nil {
					buf[i+0] = 0
				} else {
					buf[i+0] = 1

					{
						nbuf, err := (*tt).Marshal(buf[i+1:])
						if err != nil {
							return nil, err
						}
						i += uint64(len(nbuf))
					}
					i += 0
				}
			}

			i += 1

		case *OracleCoordinateTx:

			{
				if tt == nil {
					buf[i+0] = 0
				} else {
					buf[i+0] = 1

					{
						nbuf, err := (*tt).Marshal(buf[i+1:])
						if err != nil {
							return nil, err
						}
						i += uint64(len(nbuf))
					}
					i += 0
				}
			}

			i += 1

		}
	}
	{
		l := uint64(len(d.Events))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.Events {

			{
				nbuf, err := d.Events[k0].Marshal(buf[i+0:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}

		}
	}
	{

		buf[i+0+0] = byte(d.Index >> 0)

		buf[i+1+0] = byte(d.Index >> 8)

		buf[i+2+0] = byte(d.Index >> 16)

		buf[i+3+0] = byte(d.Index >> 24)

	}
	{
		if d.Final {
			buf[i+4] = 1
		} else {
			buf[i+4] = 0
		}
	}
	return buf[:i+5], nil
}

func (d *BinlogEntry) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		v := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			v = t

		}
		switch v {

		case 0 + 1:
			var tt *MySQLCoordinateTx

			{
				if buf[i+0] == 1 {
					if tt == nil {
						tt = new(MySQLCoordinateTx)
					}

					{
						ni, err := (*tt).Unmarshal(buf[i+1:])
						if err != nil {
							return 0, err
						}
						i += ni
					}
					i += 0
				} else {
					tt = nil
				}
			}

			i += 1

			d.Coordinates = tt

		case 1 + 1:
			var tt *OracleCoordinateTx

			{
				if buf[i+0] == 1 {
					if tt == nil {
						tt = new(OracleCoordinateTx)
					}

					{
						ni, err := (*tt).Unmarshal(buf[i+1:])
						if err != nil {
							return 0, err
						}
						i += ni
					}
					i += 0
				} else {
					tt = nil
				}
			}

			i += 1

			d.Coordinates = tt

		default:
			d.Coordinates = nil
		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Events)) >= l {
			d.Events = d.Events[:l]
		} else {
			d.Events = make([]DataEvent, l)
		}
		for k0 := range d.Events {

			{
				ni, err := d.Events[k0].Unmarshal(buf[i+0:])
				if err != nil {
					return 0, err
				}
				i += ni
			}

		}
	}
	{

		d.Index = 0 | (int32(buf[i+0+0]) << 0) | (int32(buf[i+1+0]) << 8) | (int32(buf[i+2+0]) << 16) | (int32(buf[i+3+0]) << 24)

	}
	{
		d.Final = buf[i+4] == 1
	}
	return i + 5, nil
}

type BinlogEntries struct {
	Entries []*BinlogEntry
}

func (d *BinlogEntries) Size() (s uint64) {

	{
		l := uint64(len(d.Entries))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Entries {

			{
				if d.Entries[k0] != nil {

					{
						s += (*d.Entries[k0]).Size()
					}
					s += 0
				}
			}

			s += 1

		}

	}
	return
}
func (d *BinlogEntries) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.Entries))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.Entries {

			{
				if d.Entries[k0] == nil {
					buf[i+0] = 0
				} else {
					buf[i+0] = 1

					{
						nbuf, err := (*d.Entries[k0]).Marshal(buf[i+1:])
						if err != nil {
							return nil, err
						}
						i += uint64(len(nbuf))
					}
					i += 0
				}
			}

			i += 1

		}
	}
	return buf[:i+0], nil
}

func (d *BinlogEntries) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Entries)) >= l {
			d.Entries = d.Entries[:l]
		} else {
			d.Entries = make([]*BinlogEntry, l)
		}
		for k0 := range d.Entries {

			{
				if buf[i+0] == 1 {
					if d.Entries[k0] == nil {
						d.Entries[k0] = new(BinlogEntry)
					}

					{
						ni, err := (*d.Entries[k0]).Unmarshal(buf[i+1:])
						if err != nil {
							return 0, err
						}
						i += ni
					}
					i += 0
				} else {
					d.Entries[k0] = nil
				}
			}

			i += 1

		}
	}
	return i + 0, nil
}

type ControlMsg struct {
	Type int32
	Msg  string
}

func (d *ControlMsg) Size() (s uint64) {

	{
		l := uint64(len(d.Msg))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 4
	return
}
func (d *ControlMsg) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{

		buf[0+0] = byte(d.Type >> 0)

		buf[1+0] = byte(d.Type >> 8)

		buf[2+0] = byte(d.Type >> 16)

		buf[3+0] = byte(d.Type >> 24)

	}
	{
		l := uint64(len(d.Msg))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+4] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+4] = byte(t)
			i++

		}
		copy(buf[i+4:], d.Msg)
		i += l
	}
	return buf[:i+4], nil
}

func (d *ControlMsg) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.Type = 0 | (int32(buf[i+0+0]) << 0) | (int32(buf[i+1+0]) << 8) | (int32(buf[i+2+0]) << 16) | (int32(buf[i+3+0]) << 24)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+4] & 0x7F)
			for buf[i+4]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+4]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.Msg = string(buf[i+4 : i+4+l])
		i += l
	}
	return i + 4, nil
}

type BigTxAck struct {
	GNO   int64
	Index int32
}

func (d *BigTxAck) Size() (s uint64) {

	s += 12
	return
}
func (d *BigTxAck) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{

		buf[0+0] = byte(d.GNO >> 0)

		buf[1+0] = byte(d.GNO >> 8)

		buf[2+0] = byte(d.GNO >> 16)

		buf[3+0] = byte(d.GNO >> 24)

		buf[4+0] = byte(d.GNO >> 32)

		buf[5+0] = byte(d.GNO >> 40)

		buf[6+0] = byte(d.GNO >> 48)

		buf[7+0] = byte(d.GNO >> 56)

	}
	{

		buf[0+8] = byte(d.Index >> 0)

		buf[1+8] = byte(d.Index >> 8)

		buf[2+8] = byte(d.Index >> 16)

		buf[3+8] = byte(d.Index >> 24)

	}
	return buf[:i+12], nil
}

func (d *BigTxAck) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.GNO = 0 | (int64(buf[0+0]) << 0) | (int64(buf[1+0]) << 8) | (int64(buf[2+0]) << 16) | (int64(buf[3+0]) << 24) | (int64(buf[4+0]) << 32) | (int64(buf[5+0]) << 40) | (int64(buf[6+0]) << 48) | (int64(buf[7+0]) << 56)

	}
	{

		d.Index = 0 | (int32(buf[0+8]) << 0) | (int32(buf[1+8]) << 8) | (int32(buf[2+8]) << 16) | (int32(buf[3+8]) << 24)

	}
	return i + 12, nil
}
