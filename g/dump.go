package g

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

func DumpLoop(logger hclog.Logger) {
	c := make(chan os.Signal, 10)
	signal.Notify(c, syscall.SIGTTIN)

	for {
		sig := <-c
		switch sig {
		case syscall.SIGTTIN:
			go CreateDump(logger)
		default:
		}
	}
}

func CreateDump(logger hclog.Logger) {
	logger.Debug("begin to create dtle dump")
	dumpPath := fmt.Sprintf("%s_%s/", "/tmp/dtle_dump", time.Now().Format("2006_01_02_15_04_05"))

	if err := os.Mkdir(dumpPath, 0755); nil != err {
		logger.Error("create dump info failed", "dump path", dumpPath, "error", err)
		return
	}
	createDump(logger, dumpPath)
	logger.Debug("create dtle dump finished")
}

func createDump(logger hclog.Logger, path string) {
	if err := captureProfile(path, "goroutine", 0); nil != err {
		logger.Error("capture goroutine profile failed", "error", err)
	}
	if err := captureProfile(path, "heap", 0); nil != err {
		logger.Error("capture heap profile failed", "error", err)
	}
	if err := captureProfile(path, "status", 0); nil != err {
		logger.Error("capture process status failed", "error", err)
	}
	return
}

func captureProfile(path, name string, extraInfo int) error {
	fileName := fmt.Sprintf("%v%v.out", path, name)
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0640)
	if nil != err {
		return fmt.Errorf("write dump error(%v)", err)
	}
	defer f.Close()

	switch name {
	case "heap":
		return pprof.Lookup("heap").WriteTo(f, 1)
	case "goroutine":
		return pprof.Lookup("goroutine").WriteTo(f, 1)
	case "status":
		return recordProcessStatus(f)
	default:
		return fmt.Errorf("not support profile %v", name)
	}
}

func recordProcessStatus(w io.Writer) error {
	pid := syscall.Getpid()
	status, err := ioutil.ReadFile(fmt.Sprintf("/proc/%v/status", pid))
	if err != nil {
		return fmt.Errorf("read /proc/%v/status error: %v", pid, err)
	}

	if _, err = w.Write(status); err != nil {
		return fmt.Errorf("record status error: %v", err)
	}
	return nil
}
