package common

import (
	"encoding/hex"
	"github.com/actiontech/dtle/helper/u"
	"github.com/hashicorp/go-hclog"
	"reflect"
	"testing"
)

func TestParseQueryEventFlags(t *testing.T) {
	logger := hclog.Default()
	logger.SetLevel(hclog.Debug)

	type args struct {
		bs []byte
	}
	bs1, err := hex.DecodeString("0000000000012000a0550000000006037374640302000100042d002d002d000c01706d5f69616d5f6964656e746974795f6462001000")
	u.PanicIfErr(err)
	bs2, err := hex.DecodeString("0000000000012000a055000000000603737464042d002d0008000cfe")
	u.PanicIfErr(err)
	tests := []struct {
		name    string
		args    args
		wantR   QueryEventFlags
		wantErr bool
	}{
		{
			name: "query-event-flag-2",
			args: args{bs2},
			wantR: QueryEventFlags{
				NoForeignKeyChecks:  false,
				CharacterSetClient:  "utf8mb4", // utf8mb4_general_ci
				CollationConnection: "utf8mb4_general_ci",
				CollationServer:     "latin1_swedish_ci",
			},
			wantErr: false,
		}, {
			name: "query-event-flag-1",
			args: args{bs1},
			wantR: QueryEventFlags{
				NoForeignKeyChecks:  false,
				CharacterSetClient:  "utf8mb4", // utf8mb4_general_ci
				CollationConnection: "utf8mb4_general_ci",
				CollationServer:     "utf8mb4_general_ci",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotR, err := ParseQueryEventFlags(tt.args.bs, logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQueryEventFlags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotR, tt.wantR) {
				t.Errorf("ParseQueryEventFlags() gotR = %v, want %v", gotR, tt.wantR)
			}
		})
	}
}
