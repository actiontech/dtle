package config

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestConfig_Parse(t *testing.T) {
	cases := []struct {
		File   string
		Result *Config
		Err    bool
	}{
		{
			"udup.conf",
			&Config{
				LogLevel:  "debug",
				LogRotate: "hour",
			},
			false,
		},
	}

	for _, tc := range cases {
		t.Logf("Testing parse: %s", tc.File)

		path, err := filepath.Abs(filepath.Join("../etc", tc.File))
		if err != nil {
			t.Fatalf("file: %s\n\n%s", tc.File, err)
			continue
		}

		actual, err := ParseConfigFile(path)
		if (err != nil) != tc.Err {
			t.Fatalf("file: %s\n\n%s", tc.File, err)
			continue
		}

		if !reflect.DeepEqual(actual, tc.Result) {
			t.Fatalf("file: %s\n\n%#v\n\n%#v", tc.File, actual, tc.Result)
		}
	}
}
