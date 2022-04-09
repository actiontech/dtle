package kafka

import (
	"encoding/base64"
	"testing"
)

func TestDecimalValueFromStringMysql(t *testing.T) {
	if DecimalValueFromStringMysql("0") != base64.StdEncoding.EncodeToString([]byte{0}) {
		t.Fail()
	}
}


func TestTimeValue(t *testing.T) {
	test := func(value string, h,m,s,microsec int64, isNeg bool) {
		if TimeValue(value) != timeValueHelper(h,m,s,microsec,isNeg) {
			t.Fatalf("failed for %v", value)
		}
	}
	test("01:02:03",1,2,3,0,false)
	test("-800:02:03.100000",800,2,3,100000,true)
}
