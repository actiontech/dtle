package base

import (
	"reflect"
	"testing"

	test "github.com/outbrain/golib/tests"
)

func TestBinlogCoordinates(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 5000}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00112", LogPos: 104}

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

func TestBinlogNext(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00018")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00099", LogPos: 104}
	cres, err = c2.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00100")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00099", LogPos: 104}
	cres, err = c3.NextFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql.00.prod.com.00100")
}

func TestBinlogPrevious(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	cres, err := c1.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00016")

	c2 := BinlogCoordinates{LogFile: "mysql-bin.00100", LogPos: 104}
	cres, err = c2.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql-bin.00099")

	c3 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00100", LogPos: 104}
	cres, err = c3.PreviousFileCoordinates()

	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(c1.Type, cres.Type)
	test.S(t).ExpectEquals(cres.LogFile, "mysql.00.prod.com.00099")

	c4 := BinlogCoordinates{LogFile: "mysql.00.prod.com.00000", LogPos: 104}
	_, err = c4.PreviousFileCoordinates()

	test.S(t).ExpectNotNil(err)
}

func TestBinlogCoordinatesAsKey(t *testing.T) {
	m := make(map[BinlogCoordinates]bool)

	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}
	c3 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c4 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 222}

	m[c1] = true
	m[c2] = true
	m[c3] = true
	m[c4] = true

	test.S(t).ExpectEquals(len(m), 3)
}

func TestBinlogFileNumber(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	c2 := BinlogCoordinates{LogFile: "mysql-bin.00022", LogPos: 104}

	test.S(t).ExpectEquals(c1.FileNumberDistance(&c1), 0)
	test.S(t).ExpectEquals(c1.FileNumberDistance(&c2), 5)
	test.S(t).ExpectEquals(c2.FileNumberDistance(&c1), -5)
}

func TestBinlogFileNumberDistance(t *testing.T) {
	c1 := BinlogCoordinates{LogFile: "mysql-bin.00017", LogPos: 104}
	fileNum, numLen := c1.FileNumber()

	test.S(t).ExpectEquals(fileNum, 17)
	test.S(t).ExpectEquals(numLen, 5)
}

func TestParseBinlogCoordinates(t *testing.T) {
	type args struct {
		logFileLogPos string
	}
	tests := []struct {
		name    string
		args    args
		want    *BinlogCoordinates
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBinlogCoordinates(tt.args.logFileLogPos)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBinlogCoordinates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseBinlogCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
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
			b := BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
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
		other *BinlogCoordinates
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
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
		other *BinlogCoordinates
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
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
		other *BinlogCoordinates
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			if got := b.SmallerThanOrEquals(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.SmallerThanOrEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_FileSmallerThan(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		other *BinlogCoordinates
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
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			if got := b.FileSmallerThan(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.FileSmallerThan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_FileNumberDistance(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		other *BinlogCoordinates
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			if got := b.FileNumberDistance(tt.args.other); got != tt.want {
				t.Errorf("BinlogCoordinates.FileNumberDistance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_FileNumber(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name   string
		fields fields
		want   int
		want1  int
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			got, got1 := b.FileNumber()
			if got != tt.want {
				t.Errorf("BinlogCoordinates.FileNumber() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("BinlogCoordinates.FileNumber() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestBinlogCoordinates_PreviousFileCoordinatesBy(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	type args struct {
		offset int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BinlogCoordinates
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			got, err := b.PreviousFileCoordinatesBy(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinlogCoordinates.PreviousFileCoordinatesBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BinlogCoordinates.PreviousFileCoordinatesBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_PreviousFileCoordinates(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name    string
		fields  fields
		want    BinlogCoordinates
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			got, err := b.PreviousFileCoordinates()
			if (err != nil) != tt.wantErr {
				t.Errorf("BinlogCoordinates.PreviousFileCoordinates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BinlogCoordinates.PreviousFileCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_NextFileCoordinates(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name    string
		fields  fields
		want    BinlogCoordinates
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			got, err := b.NextFileCoordinates()
			if (err != nil) != tt.wantErr {
				t.Errorf("BinlogCoordinates.NextFileCoordinates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BinlogCoordinates.NextFileCoordinates() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogCoordinates_DetachedCoordinates(t *testing.T) {
	type fields struct {
		LogFile string
		LogPos  int64
		GtidSet string
		Type    BinlogType
	}
	tests := []struct {
		name                string
		fields              fields
		wantIsDetached      bool
		wantDetachedLogFile string
		wantDetachedLogPos  string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BinlogCoordinates{
				LogFile: tt.fields.LogFile,
				LogPos:  tt.fields.LogPos,
				GtidSet: tt.fields.GtidSet,
				Type:    tt.fields.Type,
			}
			gotIsDetached, gotDetachedLogFile, gotDetachedLogPos := b.DetachedCoordinates()
			if gotIsDetached != tt.wantIsDetached {
				t.Errorf("BinlogCoordinates.DetachedCoordinates() gotIsDetached = %v, want %v", gotIsDetached, tt.wantIsDetached)
			}
			if gotDetachedLogFile != tt.wantDetachedLogFile {
				t.Errorf("BinlogCoordinates.DetachedCoordinates() gotDetachedLogFile = %v, want %v", gotDetachedLogFile, tt.wantDetachedLogFile)
			}
			if gotDetachedLogPos != tt.wantDetachedLogPos {
				t.Errorf("BinlogCoordinates.DetachedCoordinates() gotDetachedLogPos = %v, want %v", gotDetachedLogPos, tt.wantDetachedLogPos)
			}
		})
	}
}
