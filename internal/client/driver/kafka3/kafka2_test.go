package kafka3

import (
	"encoding/base64"
	"testing"
)

func TestDecimalValueFromStringMysql(t *testing.T) {
	if DecimalValueFromStringMysql("0") != base64.StdEncoding.EncodeToString([]byte{0}) {
		t.Fail()
	}
}
