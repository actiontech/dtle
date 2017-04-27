package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/mitchellh/cli"

	"udup/cmd/udup/command"
	"udup/agent"
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

	// Get the command line args. We shortcut "--version" and "-v" to
	// just show the version.
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
		HelpFunc: cli.BasicHelpFunc("Udup"),
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
		"agent": func() (cli.Command, error) {
			return &agent.Command{
				Version:    Version,
				Ui:         meta.Ui,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"alloc-status": func() (cli.Command, error) {
			return &command.AllocStatusCommand{
				Meta: meta,
			}, nil
		},
		"agent-info": func() (cli.Command, error) {
			return &command.AgentInfoCommand{
				Meta: meta,
			}, nil
		},
		"client-config": func() (cli.Command, error) {
			return &command.ClientConfigCommand{
				Meta: meta,
			}, nil
		},
		"eval-status": func() (cli.Command, error) {
			return &command.EvalStatusCommand{
				Meta: meta,
			}, nil
		},
		"node-status": func() (cli.Command, error) {
			return &command.NodeStatusCommand{
				Meta: meta,
			}, nil
		},
		"run": func() (cli.Command, error) {
			return &command.RunCommand{
				Meta: meta,
			}, nil
		},
		"stop": func() (cli.Command, error) {
			return &command.StopCommand{
				Meta: meta,
			}, nil
		},
		"server-force-leave": func() (cli.Command, error) {
			return &command.ServerForceLeaveCommand{
				Meta: meta,
			}, nil
		},
		"server-join": func() (cli.Command, error) {
			return &command.ServerJoinCommand{
				Meta: meta,
			}, nil
		},
		"server-members": func() (cli.Command, error) {
			return &command.ServerMembersCommand{
				Meta: meta,
			}, nil
		},
		"status": func() (cli.Command, error) {
			return &command.StatusCommand{
				Meta: meta,
			}, nil
		},
		"validate": func() (cli.Command, error) {
			return &command.ValidateCommand{
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
