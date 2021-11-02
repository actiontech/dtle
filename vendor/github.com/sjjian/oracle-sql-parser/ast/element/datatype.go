package element

type Datatype interface {
	DataDef() DataDef
}

type DataDef int

const (
	DataDefChar DataDef = iota
	DataDefVarchar2
	DataDefNChar
	DataDefNVarChar2
	DataDefNumber
	DataDefFloat
	DataDefBinaryFloat
	DataDefBinaryDouble
	DataDefLong
	DataDefLongRaw
	DataDefRaw
	DataDefDate
	DataDefTimestamp
	DataDefIntervalYear
	DataDefIntervalDay
	DataDefBlob
	DataDefClob
	DataDefNClob
	DataDefBFile
	DataDefRowId
	DataDefURowId
	DataDefCharacter
	DataDefCharacterVarying
	DataDefCharVarying
	DataDefNCharVarying
	DataDefVarchar
	DataDefNationalCharacter
	DataDefNationalCharacterVarying
	DataDefNationalChar
	DataDefNationalCharVarying
	DataDefNumeric
	DataDefDecimal
	DataDefDec
	DataDefInteger
	DataDefInt
	DataDefSmallInt
	DataDefDoublePrecision
	DataDefReal
)

type datatype struct {
	typ DataDef
}

func (d *datatype) DataDef () DataDef {
	return d.typ
}

func (d *datatype) SetDataDef(typ DataDef) {
	d.typ = typ
}

// Char is "Char" and "Character"
type Char struct {
	datatype
	Size       *int
	IsByteSize bool
	IsCharSize bool
}

// Varchar2 include: "Varchar2", "Char Varying", "Character Varying", "Varchar"
type Varchar2 struct {
	Char
}

// NChar include: "Nchar", "National Character", "National Char".
type NChar struct {
	datatype
	Size      *int
}

// NVarchar2 include: "NVarchar2", "National Character Varying", "National Char Varying", "NChar Varying"
type NVarchar2 struct {
	NChar
}

type Raw struct {
	datatype
	Size *int
}

type RowId struct {
	datatype
}

type URowId struct {
	datatype
	Size *int
}

type Date struct {
	datatype
}

type Timestamp struct {
	datatype
	FractionalSecondsPrecision *int
	WithTimeZone               bool
	WithLocalTimeZone          bool
}

type IntervalYear struct {
	datatype
	Precision *int
}

type IntervalDay struct {
	datatype
	Precision          			*int
	FractionalSecondsPrecision 	*int
}

// Number include: "Number", "Numeric", "Decimal", "Dec", "Integer", "Int", "Smallint";
// Integer is a alias of Number(38);
// Int is  a alias of Number(38);
// Smallint is  a alias of Number(38)
type Number struct {
	datatype
	Precision *NumberOrAsterisk
	Scale     *int
}

// Float is a subtype of Number, include: "Float", "", "DoublePrecision", "Real";
// DoublePrecision is a alias of FLOAT(126);
// Real is a alias of FLOAT(63).
type Float struct {
	datatype
	Precision *NumberOrAsterisk
}

type BinaryFloat struct {
	datatype
}

type BinaryDouble struct {
	datatype
}

type Long struct {
	datatype
}

type LongRaw struct {
	datatype
}

type Blob struct {
	datatype
}

type Clob struct {
	datatype
}

type NClob struct {
	datatype
}

type BFile struct {
	datatype
}