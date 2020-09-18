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

type BinlogCoordinateTx struct {
	LogFile       string
	LogPos        int64
	OSID          string
	SID           [16]byte
	GNO           int64
	LastCommitted int64
	SeqenceNumber int64
}

func (d *BinlogCoordinateTx) Size() (s uint64) {

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
func (d *BinlogCoordinateTx) Marshal(buf []byte) ([]byte, error) {
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

func (d *BinlogCoordinateTx) Unmarshal(buf []byte) (uint64, error) {
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
