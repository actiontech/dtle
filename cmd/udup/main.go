package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"

	"udup/cmd/udup/command"
)

// The git commit that was compiled. This will be filled in by the compiler.
var (
	Version   string
	GitBranch string
	GitCommit string
)

func main() {
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

	ui := &cli.BasicUi{
		Reader:      os.Stdin,
		Writer:      os.Stdout,
		ErrorWriter: os.Stderr,
	}

	c.Commands = map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &command.AgentCommand{
				Ui:         ui,
				Version:    Version,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"version": func() (cli.Command, error) {
			return &command.VersionCommand{
				Version: Version,
				Commit:  GitCommit,
				Branch:  GitBranch,
				Ui:      ui,
			}, nil
		},
	}

	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(exitCode)
}
