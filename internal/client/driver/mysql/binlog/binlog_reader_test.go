package binlog

import (
	gomysql "github.com/siddontang/go-mysql/mysql"
	"testing"
)

func TestParseMysqlGTIDSet(t *testing.T) {
	type args struct {
		gtidset string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"t1", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:1:2:3:4:6:5"}, false},
		{"t2", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5,de278ad0-2106-11e4-9f8e-6edd0ca20947:6-7:10-20"}, false},
		{"t3", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:6-7:1-4:10-20"}, false},
		{"t4", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:14972:14977:14983:14984:14989:14992"}, false},
		{"t5", args{"de278ad0-2106-11e4-9f8e-6edd0ca20947:38137-38142,de278ad0-2106-11e4-9f8e-6edd0ca20947:38137-38143"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGtidset, err := gomysql.ParseMysqlGTIDSet(tt.args.gtidset)
			if err != nil {
				t.Errorf("ParseMysqlGTIDSet error = %v", err)
				return
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMysqlGTIDSet error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			println(t.Name(), gotGtidset.String())
		})
	}
}
