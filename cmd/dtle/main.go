/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"os"

	"github.com/actiontech/dtle/agent"
	"github.com/actiontech/dtle/cmd/dtle/command"
	"github.com/mitchellh/cli"
					"syscall"
		"os/signal"
	"time"
	"runtime/pprof"
	"runtime"
	"io"
)

// The git commit that was compiled. This will be filled in by the compiler.
var (
	Version   string
	GitBranch string
	GitCommit string
)


func main() {
	go DumpLoop()
	os.Exit(realMain())
}

func realMain() int {
	log.SetOutput(ioutil.Discard)
	args := os.Args[1:]
	for _, arg := range args {
		if arg == "-v" || arg == "--version" {
			newArgs := make([]string, len(args)+1)
			newArgs[0] = "version"
			copy(newArgs[1:], args)
			args = newArgs
			break
		}
	}

	c := &cli.CLI{
		Args:     args,
		HelpFunc: cli.BasicHelpFunc("Dtle"),
	}

	meta := command.Meta{}
	if meta.Ui == nil {
		meta.Ui = &cli.BasicUi{
			Reader:      os.Stdin,
			Writer:      os.Stdout,
			ErrorWriter: os.Stderr,
		}
	}

	c.Commands = map[string]cli.CommandFactory{
		"server": func() (cli.Command, error) {
			return &agent.Command{
				Version:    Version,
				Revision:   GitCommit,
				Ui:         meta.Ui,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		/*"init": func() (cli.Command, error) {
			return &command.InitCommand{
				Meta: meta,
			}, nil
		},*/
		/*"agent-config": func() (cli.Command, error) {
			return &command.ConfigCommand{
				Meta: meta,
			}, nil
		},*/
		"node-status": func() (cli.Command, error) {
			return &command.NodeStatusCommand{
				Meta: meta,
			}, nil
		},
		"members": func() (cli.Command, error) {
			return &command.ServerMembersCommand{
				Meta: meta,
			}, nil
		},
		/*"server-force-leave": func() (cli.Command, error) {
			return &command.ServerForceLeaveCommand{
				Meta: meta,
			}, nil
		},
		"remove-peer": func() (cli.Command, error) {
			return &command.OperatorRaftRemoveCommand{
				Meta: meta,
			}, nil
		},
		"start": func() (cli.Command, error) {
			return &command.StartCommand{
				Meta: meta,
			}, nil
		},
		"stop": func() (cli.Command, error) {
			return &command.StopCommand{
				Meta: meta,
			}, nil
		},*/
		"job-status": func() (cli.Command, error) {
			return &command.StatusCommand{
				Meta: meta,
			}, nil
		},
		"version": func() (cli.Command, error) {
			return &command.VersionCommand{
				Version: Version,
				Commit:  GitCommit,
				Branch:  GitBranch,
				Ui:      meta.Ui,
			}, nil
		},
	}

	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}
func DumpLoop() {
	c := make(chan os.Signal, 10)
	signal.Notify(c, syscall.Signal(0x15)) //0x15=SIGTTIN

	for {
		sig := <-c
		switch sig {
		case syscall.Signal(0x15):
			go CreateDump()
		default:
		}
	}
}


func CreateDump() {
	dumpPath := fmt.Sprintf("%s_%s/", "./logs/dtle_dump", time.Now().Format("2006_01_02_15_04_05"))

	if err := os.Mkdir(dumpPath, 0755); nil != err {
		fmt.Fprintf(os.Stderr, "mkdir %v error: %v", dumpPath, err)
		return
	}
	createDump(dumpPath)
}

func createDump(path string) {
	if err := captureProfile(path, "goroutine", 0); nil != err {
		fmt.Fprintf(os.Stderr, "capture goroutine profile error(%v)\n", err)
	}
	if err := captureProfile(path, "heap", 0); nil != err {
		fmt.Fprintf(os.Stderr, "capture heap profile error(%v)\n", err)
	}
	if err := captureProfile(path, "status", 0); nil != err {
		fmt.Fprintf(os.Stderr, "capture process status error(%v)\n", err)
	}
	return

}

func captureProfile(path, name string, extraInfo int) error {
	f, err := os.OpenFile(path+name+".out", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0640)
	if nil != err {
		return fmt.Errorf("write dump error(%v)", err)
	}
	defer f.Close()
	switch name {
	case "cpu":
		if extraInfo <= 0 {
			extraInfo = 30
		}
		if err := pprof.StartCPUProfile(f); nil != err {
			return err
		}
		time.Sleep(time.Duration(extraInfo) * time.Second)
		pprof.StopCPUProfile()
	case "heap":
		return pprof.Lookup("heap").WriteTo(f, 1)
	case "mutex":
		runtime.SetMutexProfileFraction(extraInfo)
		return pprof.Lookup("mutex").WriteTo(f, 1)
	case "block":
		runtime.SetBlockProfileRate(extraInfo)
		return pprof.Lookup("block").WriteTo(f, 1)
	case "goroutine":
		return pprof.Lookup("goroutine").WriteTo(f, 1)
	case "threadcreate":
		return pprof.Lookup("threadcreate").WriteTo(f, 1)
	case "status":
		return recordProcessStatus(f)
	default:
		return fmt.Errorf("not support profile %v", name)
	}
	return nil
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