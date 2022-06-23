package common

import (
	"reflect"

	uuid "github.com/satori/go.uuid"
)

type CoordinatesI interface{
	GetSid()interface{}
	GetSidStr() string
	GetGtidForThisTx()string
	GetFieldValue(fieldName string)interface{}
	SetField(fieldName string,fieldValue interface{})
	GetLogPos()int64
	GetLastCommit()int64
	GetGNO()int64
	GetSequenceNumber()int64
	GetLogFile()string
	GetOSID()string
}

// todo Support key value general settings  
func GetFieldValue(fieldName string)interface{}{
   return nil
}

func SetField(fieldName string,fieldValue interface{}){
    return 
}



// Oracle CoordinateTx

func (o *OracleCoordinateTx)GetSid() interface{} {
	return uuid.UUID([16]byte{0})
}

func (o *OracleCoordinateTx)GetSidStr() string {
	return uuid.UUID([16]byte{0}).String()
}

func (o *OracleCoordinateTx) GetGtidForThisTx() string {
	return ""
}

func (o *OracleCoordinateTx)GetFieldValue(fieldName string)interface{}{
	v := reflect.ValueOf(*o)
	return v.FieldByName(fieldName)
 }
 
 func (o *OracleCoordinateTx)SetField(fieldName string,fieldValue interface{}){
	v := reflect.ValueOf(&o).Elem().Elem()
	v.FieldByName(fieldName).Set(reflect.ValueOf(fieldValue)) 
 }

 func (o *OracleCoordinateTx)GetLogPos()int64{
	return o.OldestUncommittedScn
 }

 func (o *OracleCoordinateTx)GetLastCommit()int64{
	return o.EndSCN
 }

 func (o *OracleCoordinateTx)GetGNO()int64{
	return 0
 }

 func (b *OracleCoordinateTx)GetSequenceNumber()int64{
	return 0
 }

 func (o *OracleCoordinateTx)GetLogFile()string{
	return ""
 }

 func (o *OracleCoordinateTx)GetOSID()string{
	return ""
 }
 type DumpCoordinates interface{
	GetLogPos() int64
	GetTxSet() string
	GetLogFile() string
}


func (b *OracleCoordinates) GetLogPos() int64 {
	return b.LaststSCN
}

func (b *OracleCoordinates) GetTxSet() string {
	return ""
}

func (b *OracleCoordinates) GetLogFile() string {
	return ""
}
