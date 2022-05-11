package kafka

import "testing"

func Test_getBinaryValue(t *testing.T) {
	type args struct {
		binary string
		value  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "binary1",
			args: args{
				binary: "binary(16)",
				value:  "",
			},
			want: "AAAAAAAAAAAAAAAAAAAAAA==",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBinaryValue(tt.args.binary, tt.args.value); got != tt.want {
				t.Errorf("getBinaryValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getBitValue(t *testing.T) {
	type args struct {
		bit   string
		value int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "bit8-0",
			args: args{"bit(8)", 0},
			want: "AA==",
		},
		{
			name: "bit8-9",
			args: args{"bit(8)", 9},
			want: "CQ==",
		},
		{
			name: "bit16-9",
			args: args{"bit(16)", 9},
			want: "CQA=",
		},
		{
			name: "bit16-9",
			args: args{"bit(16)", 9},
			want: "CQA=",
		},
		{
			name: "bit16-19",
			args: args{"bit(16)", 19},
			want: "EwA=",
		},
		{
			name: "bit59-9",
			args: args{"bit(59)", 9},
			want: "CQAAAAAAAAA=",
		},
		{
			name: "bit59-19",
			args: args{"bit(59)", 19},
			want: "EwAAAAAAAAA=",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBitValue(tt.args.bit, tt.args.value); got != tt.want {
				t.Errorf("getBitValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
