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
