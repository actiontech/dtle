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

type ColumnValues struct {
	AbstractValues []*interface{}
}

func (d *ColumnValues) Size() (s uint64) {

	{
		l := uint64(len(d.AbstractValues))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.AbstractValues {

			{
				if d.AbstractValues[k0] != nil {

					{
						var v uint64
						switch (*d.AbstractValues[k0]).(type) {

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
						switch tt := (*d.AbstractValues[k0]).(type) {

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
					s += 0
				}
			}

			s += 1

		}

	}
	return
}
func (d *ColumnValues) Marshal(buf []byte) ([]byte, error) {
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
		l := uint64(len(d.AbstractValues))

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
		for k0 := range d.AbstractValues {

			{
				if d.AbstractValues[k0] == nil {
					buf[i+0] = 0
				} else {
					buf[i+0] = 1

					{
						var v uint64
						switch (*d.AbstractValues[k0]).(type) {

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
								buf[i+1] = byte(t) | 0x80
								t >>= 7
								i++
							}
							buf[i+1] = byte(t)
							i++

						}
						switch tt := (*d.AbstractValues[k0]).(type) {

						case string:

							{
								l := uint64(len(tt))

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
								copy(buf[i+1:], tt)
								i += l
							}

						case int8:

							{

								buf[i+0+1] = byte(tt >> 0)

							}

							i += 1

						case int16:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

							}

							i += 2

						case int32:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

								buf[i+2+1] = byte(tt >> 16)

								buf[i+3+1] = byte(tt >> 24)

							}

							i += 4

						case int64:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

								buf[i+2+1] = byte(tt >> 16)

								buf[i+3+1] = byte(tt >> 24)

								buf[i+4+1] = byte(tt >> 32)

								buf[i+5+1] = byte(tt >> 40)

								buf[i+6+1] = byte(tt >> 48)

								buf[i+7+1] = byte(tt >> 56)

							}

							i += 8

						case uint8:

							{

								buf[i+0+1] = byte(tt >> 0)

							}

							i += 1

						case uint16:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

							}

							i += 2

						case uint32:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

								buf[i+2+1] = byte(tt >> 16)

								buf[i+3+1] = byte(tt >> 24)

							}

							i += 4

						case uint64:

							{

								buf[i+0+1] = byte(tt >> 0)

								buf[i+1+1] = byte(tt >> 8)

								buf[i+2+1] = byte(tt >> 16)

								buf[i+3+1] = byte(tt >> 24)

								buf[i+4+1] = byte(tt >> 32)

								buf[i+5+1] = byte(tt >> 40)

								buf[i+6+1] = byte(tt >> 48)

								buf[i+7+1] = byte(tt >> 56)

							}

							i += 8

						case []byte:

							{
								l := uint64(len(tt))

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
								copy(buf[i+1:], tt)
								i += l
							}

						case float32:

							{

								v := *(*uint32)(unsafe.Pointer(&(tt)))

								buf[i+0+1] = byte(v >> 0)

								buf[i+1+1] = byte(v >> 8)

								buf[i+2+1] = byte(v >> 16)

								buf[i+3+1] = byte(v >> 24)

							}

							i += 4

						case float64:

							{

								v := *(*uint64)(unsafe.Pointer(&(tt)))

								buf[i+0+1] = byte(v >> 0)

								buf[i+1+1] = byte(v >> 8)

								buf[i+2+1] = byte(v >> 16)

								buf[i+3+1] = byte(v >> 24)

								buf[i+4+1] = byte(v >> 32)

								buf[i+5+1] = byte(v >> 40)

								buf[i+6+1] = byte(v >> 48)

								buf[i+7+1] = byte(v >> 56)

							}

							i += 8

						case bool:

							{
								if tt {
									buf[i+1] = 1
								} else {
									buf[i+1] = 0
								}
							}

							i += 1

						}
					}
					i += 0
				}
			}

			i += 1

		}
	}
	return buf[:i+0], nil
}

func (d *ColumnValues) Unmarshal(buf []byte) (uint64, error) {
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
		if uint64(cap(d.AbstractValues)) >= l {
			d.AbstractValues = d.AbstractValues[:l]
		} else {
			d.AbstractValues = make([]*interface{}, l)
		}
		for k0 := range d.AbstractValues {

			{
				if buf[i+0] == 1 {
					if d.AbstractValues[k0] == nil {
						d.AbstractValues[k0] = new(interface{})
					}

					{
						v := uint64(0)

						{

							bs := uint8(7)
							t := uint64(buf[i+1] & 0x7F)
							for buf[i+1]&0x80 == 0x80 {
								i++
								t |= uint64(buf[i+1]&0x7F) << bs
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
									t := uint64(buf[i+1] & 0x7F)
									for buf[i+1]&0x80 == 0x80 {
										i++
										t |= uint64(buf[i+1]&0x7F) << bs
										bs += 7
									}
									i++

									l = t

								}
								tt = string(buf[i+1 : i+1+l])
								i += l
							}

							(*d.AbstractValues[k0]) = tt

						case 1 + 1:
							var tt int8

							{

								tt = 0 | (int8(buf[i+0+1]) << 0)

							}

							i += 1

							(*d.AbstractValues[k0]) = tt

						case 2 + 1:
							var tt int16

							{

								tt = 0 | (int16(buf[i+0+1]) << 0) | (int16(buf[i+1+1]) << 8)

							}

							i += 2

							(*d.AbstractValues[k0]) = tt

						case 3 + 1:
							var tt int32

							{

								tt = 0 | (int32(buf[i+0+1]) << 0) | (int32(buf[i+1+1]) << 8) | (int32(buf[i+2+1]) << 16) | (int32(buf[i+3+1]) << 24)

							}

							i += 4

							(*d.AbstractValues[k0]) = tt

						case 4 + 1:
							var tt int64

							{

								tt = 0 | (int64(buf[i+0+1]) << 0) | (int64(buf[i+1+1]) << 8) | (int64(buf[i+2+1]) << 16) | (int64(buf[i+3+1]) << 24) | (int64(buf[i+4+1]) << 32) | (int64(buf[i+5+1]) << 40) | (int64(buf[i+6+1]) << 48) | (int64(buf[i+7+1]) << 56)

							}

							i += 8

							(*d.AbstractValues[k0]) = tt

						case 5 + 1:
							var tt uint8

							{

								tt = 0 | (uint8(buf[i+0+1]) << 0)

							}

							i += 1

							(*d.AbstractValues[k0]) = tt

						case 6 + 1:
							var tt uint16

							{

								tt = 0 | (uint16(buf[i+0+1]) << 0) | (uint16(buf[i+1+1]) << 8)

							}

							i += 2

							(*d.AbstractValues[k0]) = tt

						case 7 + 1:
							var tt uint32

							{

								tt = 0 | (uint32(buf[i+0+1]) << 0) | (uint32(buf[i+1+1]) << 8) | (uint32(buf[i+2+1]) << 16) | (uint32(buf[i+3+1]) << 24)

							}

							i += 4

							(*d.AbstractValues[k0]) = tt

						case 8 + 1:
							var tt uint64

							{

								tt = 0 | (uint64(buf[i+0+1]) << 0) | (uint64(buf[i+1+1]) << 8) | (uint64(buf[i+2+1]) << 16) | (uint64(buf[i+3+1]) << 24) | (uint64(buf[i+4+1]) << 32) | (uint64(buf[i+5+1]) << 40) | (uint64(buf[i+6+1]) << 48) | (uint64(buf[i+7+1]) << 56)

							}

							i += 8

							(*d.AbstractValues[k0]) = tt

						case 9 + 1:
							var tt []byte

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
								if uint64(cap(tt)) >= l {
									tt = tt[:l]
								} else {
									tt = make([]byte, l)
								}
								copy(tt, buf[i+1:])
								i += l
							}

							(*d.AbstractValues[k0]) = tt

						case 10 + 1:
							var tt float32

							{

								v := 0 | (uint32(buf[i+0+1]) << 0) | (uint32(buf[i+1+1]) << 8) | (uint32(buf[i+2+1]) << 16) | (uint32(buf[i+3+1]) << 24)
								tt = *(*float32)(unsafe.Pointer(&v))

							}

							i += 4

							(*d.AbstractValues[k0]) = tt

						case 11 + 1:
							var tt float64

							{

								v := 0 | (uint64(buf[i+0+1]) << 0) | (uint64(buf[i+1+1]) << 8) | (uint64(buf[i+2+1]) << 16) | (uint64(buf[i+3+1]) << 24) | (uint64(buf[i+4+1]) << 32) | (uint64(buf[i+5+1]) << 40) | (uint64(buf[i+6+1]) << 48) | (uint64(buf[i+7+1]) << 56)
								tt = *(*float64)(unsafe.Pointer(&v))

							}

							i += 8

							(*d.AbstractValues[k0]) = tt

						case 12 + 1:
							var tt bool

							{
								tt = buf[i+1] == 1
							}

							i += 1

							(*d.AbstractValues[k0]) = tt

						default:
							(*d.AbstractValues[k0]) = nil
						}
					}
					i += 0
				} else {
					d.AbstractValues[k0] = nil
				}
			}

			i += 1

		}
	}
	return i + 0, nil
}

type DumpStatResult struct {
	Gtid    string
	LogFile string
	LogPos  int64
}

func (d *DumpStatResult) Size() (s uint64) {

	{
		l := uint64(len(d.Gtid))

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
	s += 8
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
		l := uint64(len(d.Gtid))

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
		copy(buf[i+0:], d.Gtid)
		i += l
	}
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
	return buf[:i+8], nil
}

func (d *DumpStatResult) Unmarshal(buf []byte) (uint64, error) {
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
		d.Gtid = string(buf[i+0 : i+0+l])
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
		d.LogFile = string(buf[i+0 : i+0+l])
		i += l
	}
	{

		d.LogPos = 0 | (int64(buf[i+0+0]) << 0) | (int64(buf[i+1+0]) << 8) | (int64(buf[i+2+0]) << 16) | (int64(buf[i+3+0]) << 24) | (int64(buf[i+4+0]) << 32) | (int64(buf[i+5+0]) << 40) | (int64(buf[i+6+0]) << 48) | (int64(buf[i+7+0]) << 56)

	}
	return i + 8, nil
}

type DataEvent struct {
	Query             string
	CurrentSchema     string
	DatabaseName      string
	TableName         string
	DML               int8
	ColumnCount       uint64
	WhereColumnValues *ColumnValues
	NewColumnValues   *ColumnValues
	Table             []byte
	LogPos            int64
	Timestamp         uint32
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
		if d.WhereColumnValues != nil {

			{
				s += (*d.WhereColumnValues).Size()
			}
			s += 0
		}
	}
	{
		if d.NewColumnValues != nil {

			{
				s += (*d.NewColumnValues).Size()
			}
			s += 0
		}
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
	s += 23
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
		if d.WhereColumnValues == nil {
			buf[i+9] = 0
		} else {
			buf[i+9] = 1

			{
				nbuf, err := (*d.WhereColumnValues).Marshal(buf[i+10:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}
			i += 0
		}
	}
	{
		if d.NewColumnValues == nil {
			buf[i+10] = 0
		} else {
			buf[i+10] = 1

			{
				nbuf, err := (*d.NewColumnValues).Marshal(buf[i+11:])
				if err != nil {
					return nil, err
				}
				i += uint64(len(nbuf))
			}
			i += 0
		}
	}
	{
		l := uint64(len(d.Table))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+11] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+11] = byte(t)
			i++

		}
		copy(buf[i+11:], d.Table)
		i += l
	}
	{

		buf[i+0+11] = byte(d.LogPos >> 0)

		buf[i+1+11] = byte(d.LogPos >> 8)

		buf[i+2+11] = byte(d.LogPos >> 16)

		buf[i+3+11] = byte(d.LogPos >> 24)

		buf[i+4+11] = byte(d.LogPos >> 32)

		buf[i+5+11] = byte(d.LogPos >> 40)

		buf[i+6+11] = byte(d.LogPos >> 48)

		buf[i+7+11] = byte(d.LogPos >> 56)

	}
	{

		buf[i+0+19] = byte(d.Timestamp >> 0)

		buf[i+1+19] = byte(d.Timestamp >> 8)

		buf[i+2+19] = byte(d.Timestamp >> 16)

		buf[i+3+19] = byte(d.Timestamp >> 24)

	}
	return buf[:i+23], nil
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
		if buf[i+9] == 1 {
			if d.WhereColumnValues == nil {
				d.WhereColumnValues = new(ColumnValues)
			}

			{
				ni, err := (*d.WhereColumnValues).Unmarshal(buf[i+10:])
				if err != nil {
					return 0, err
				}
				i += ni
			}
			i += 0
		} else {
			d.WhereColumnValues = nil
		}
	}
	{
		if buf[i+10] == 1 {
			if d.NewColumnValues == nil {
				d.NewColumnValues = new(ColumnValues)
			}

			{
				ni, err := (*d.NewColumnValues).Unmarshal(buf[i+11:])
				if err != nil {
					return 0, err
				}
				i += ni
			}
			i += 0
		} else {
			d.NewColumnValues = nil
		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+11] & 0x7F)
			for buf[i+11]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+11]&0x7F) << bs
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
		copy(d.Table, buf[i+11:])
		i += l
	}
	{

		d.LogPos = 0 | (int64(buf[i+0+11]) << 0) | (int64(buf[i+1+11]) << 8) | (int64(buf[i+2+11]) << 16) | (int64(buf[i+3+11]) << 24) | (int64(buf[i+4+11]) << 32) | (int64(buf[i+5+11]) << 40) | (int64(buf[i+6+11]) << 48) | (int64(buf[i+7+11]) << 56)

	}
	{

		d.Timestamp = 0 | (uint32(buf[i+0+19]) << 0) | (uint32(buf[i+1+19]) << 8) | (uint32(buf[i+2+19]) << 16) | (uint32(buf[i+3+19]) << 24)

	}
	return i + 23, nil
}

type BinlogEntry struct {
	Coordinates BinlogCoordinateTx
	Events      []DataEvent
}

func (d *BinlogEntry) Size() (s uint64) {

	{
		s += d.Coordinates.Size()
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
		nbuf, err := d.Coordinates.Marshal(buf[0:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(nbuf))
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
	return buf[:i+0], nil
}

func (d *BinlogEntry) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		ni, err := d.Coordinates.Unmarshal(buf[i+0:])
		if err != nil {
			return 0, err
		}
		i += ni
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
	return i + 0, nil
}

type BinlogEntries struct {
	Entries []*BinlogEntry
	BigTx   bool
	TxNum   int64
	TxLen   int64
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
	s += 17
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
	{
		if d.BigTx {
			buf[i+0] = 1
		} else {
			buf[i+0] = 0
		}
	}
	{

		buf[i+0+1] = byte(d.TxNum >> 0)

		buf[i+1+1] = byte(d.TxNum >> 8)

		buf[i+2+1] = byte(d.TxNum >> 16)

		buf[i+3+1] = byte(d.TxNum >> 24)

		buf[i+4+1] = byte(d.TxNum >> 32)

		buf[i+5+1] = byte(d.TxNum >> 40)

		buf[i+6+1] = byte(d.TxNum >> 48)

		buf[i+7+1] = byte(d.TxNum >> 56)

	}
	{

		buf[i+0+9] = byte(d.TxLen >> 0)

		buf[i+1+9] = byte(d.TxLen >> 8)

		buf[i+2+9] = byte(d.TxLen >> 16)

		buf[i+3+9] = byte(d.TxLen >> 24)

		buf[i+4+9] = byte(d.TxLen >> 32)

		buf[i+5+9] = byte(d.TxLen >> 40)

		buf[i+6+9] = byte(d.TxLen >> 48)

		buf[i+7+9] = byte(d.TxLen >> 56)

	}
	return buf[:i+17], nil
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
	{
		d.BigTx = buf[i+0] == 1
	}
	{

		d.TxNum = 0 | (int64(buf[i+0+1]) << 0) | (int64(buf[i+1+1]) << 8) | (int64(buf[i+2+1]) << 16) | (int64(buf[i+3+1]) << 24) | (int64(buf[i+4+1]) << 32) | (int64(buf[i+5+1]) << 40) | (int64(buf[i+6+1]) << 48) | (int64(buf[i+7+1]) << 56)

	}
	{

		d.TxLen = 0 | (int64(buf[i+0+9]) << 0) | (int64(buf[i+1+9]) << 8) | (int64(buf[i+2+9]) << 16) | (int64(buf[i+3+9]) << 24) | (int64(buf[i+4+9]) << 32) | (int64(buf[i+5+9]) << 40) | (int64(buf[i+6+9]) << 48) | (int64(buf[i+7+9]) << 56)

	}
	return i + 17, nil
}
