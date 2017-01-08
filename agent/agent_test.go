package agent

import (
	"io/ioutil"
	"os"
	"testing"

	uconf "udup/config"
)

func tmpDir(t testing.TB) string {
	dir, err := ioutil.TempDir("", "udup")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return dir
}

func makeAgent(t testing.TB, cb func(*uconf.Config)) (string, *Agent) {
	dir := tmpDir(t)

	// Customize the server configuration
	config := uconf.DefaultConfig()

	agent, err := NewAgent(config)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("err: %v", err)
	}
	return dir, agent
}
