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
)

// The git commit that was compiled. This will be filled in by the compiler.
var (
	Version   string
	GitBranch string
	GitCommit string
)

func main() {
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
