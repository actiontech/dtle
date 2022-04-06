package common

import "reflect"

type CoordinatesI interface{
	GetSid()string
	GetGtidForThisTx()string
	GetFieldValue(fieldName string)interface{}
	SetField(fieldName string,fieldValue interface{})
	GetLogPos()int64
	GetLastCommit()int64
}

// todo Support key value general settings  
func GetFieldValue(fieldName string)interface{}{
   return nil
}

func SetField(fieldName string,fieldValue interface{}){
    return 
}



// Oracle CoordinateTx

func (o *OracleCoordinateTx)GetSid() string {
	return ""
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