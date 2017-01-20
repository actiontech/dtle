package config

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestConfig_ParseConfigFile(t *testing.T) {
	// Fails if the file doesn't exist
	if _, err := ParseConfigFile("C:\\Users\\workspace\\src\\udup\\etc\\udup.conf"); err == nil {
		t.Fatalf("expected error, got nothing")
	}

	fh, err := ioutil.TempFile("", "udup")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(fh.Name())

	// Invalid content returns error
	if _, err := fh.WriteString("nope;!!!"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := ParseConfigFile(fh.Name()); err == nil {
		t.Fatalf("expected load error, got nothing")
	}

	// Valid content parses successfully
	if err := fh.Truncate(0); err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := fh.Seek(0, 0); err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := fh.WriteString(`{"log_level":"info"}`); err != nil {
		t.Fatalf("err: %s", err)
	}

	config, err := ParseConfigFile(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if config.LogLevel != "info" {
		t.Fatalf("bad loglevel: %q", config.LogLevel)
	}
}

func TestConfig_LoadConfig(t *testing.T) {
	// Fails if the target doesn't exist
	if _, err := LoadConfig("C:\\Users\\workspace\\src\\udup\\etc\\udup.conf"); err == nil {
		t.Fatalf("expected error, got nothing")
	}

	fh, err := ioutil.TempFile("", "udup")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(fh.Name())

	if _, err := fh.WriteString(`{"log_level":"info"}`); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Works on a config file
	config, err := LoadConfig(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if config.LogLevel != "info" {
		t.Fatalf("bad: %#v", config)
	}
}
