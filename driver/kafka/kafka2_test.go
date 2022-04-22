package kafka

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestDecimalValueFromStringMysql(t *testing.T) {
	if DecimalValueFromStringMysql("0") != base64.StdEncoding.EncodeToString([]byte{0}) {
		t.Fail()
	}
}

func TestTimeValue(t *testing.T) {
	test := func(value string, h, m, s, microsec int64, isNeg bool) {
		if TimeValue(value) != timeValueHelper(h, m, s, microsec, isNeg) {
			t.Fatalf("failed for %v", value)
		}
	}
	test("01:02:03", 1, 2, 3, 0, false)
	test("-800:02:03.100000", 800, 2, 3, 100000, true)
}

func TestDateTimeValue(t *testing.T) {
	tests := []struct {
		dateTime string
		want     int64
	}{
		{
			dateTime: "9999-12-31 23:59:59",
			want:     253402300799000,
		},
		{
			dateTime: "1900-03-02 04:06:09",
			want:     -2203790031000,
		},
		{
			dateTime: "5900-03-02 04:06:09",
			want:     124024017969000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.dateTime, func(t *testing.T) {
			realDateTime := DateTimeValue(tt.dateTime, time.Local)
			if realDateTime != tt.want {
				t.Errorf("parse dateTime  =  %v, want %v", realDateTime, tt.want)
			}
		})
	}
}
