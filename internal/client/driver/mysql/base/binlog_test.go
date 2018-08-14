package base

import (
	"testing"

	test "github.com/outbrain/golib/tests"
)

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinateTx{LogFile: "mysql-bin.00112", LogPos: 104}

	test.S(t).ExpectTrue(c1.Equals(&c2))
	test.S(t).ExpectFalse(c1.Equals(&c3))
	test.S(t).ExpectFalse(c1.Equals(&c4))
	test.S(t).ExpectFalse(c1.SmallerThan(&c2))
	test.S(t).ExpectTrue(c1.SmallerThan(&c3))
	test.S(t).ExpectTrue(c1.SmallerThan(&c4))
	test.S(t).ExpectTrue(c3.SmallerThan(&c4))
	test.S(t).ExpectFalse(c3.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c2))
	test.S(t).ExpectFalse(c4.SmallerThan(&c3))

	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c2))
	test.S(t).ExpectTrue(c1.SmallerThanOrEquals(&c3))
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinateTx]bool)

	c1 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinateTx{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := BinlogCoordinateTx{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}

func TestBinlogCoordinates_DisplayString(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.DisplayString(); got != tt.want {
				t.Errorf("BinlogCoordinates.DisplayString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_String(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.String(); got != tt.want {
				t.Errorf("BinlogCoordinates.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_Equals(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		other *BinlogCoordinateTx
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.Equals(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_IsEmpty(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("BinlogCoordinates.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_SmallerThan(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		other *BinlogCoordinateTx
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.SmallerThan(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.SmallerThan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_SmallerThanOrEquals(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		other *BinlogCoordinateTx
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinateTx{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
			}
			if got := b.SmallerThanOrEquals(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.SmallerThanOrEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}
