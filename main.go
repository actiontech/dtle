package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"
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
			return &AgentCommand{
				Ui:         ui,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"version": func() (cli.Command, error) {
			ver := Version
			rel := VersionPrerelease
			if GitDescribe != "" {
				ver = GitDescribe
				// Trim off a leading 'v', we append it anyways.
				if ver[0] == 'v' {
					ver = ver[1:]
				}
			}
			if GitDescribe == "" && rel == "" && VersionPrerelease != "" {
				rel = "dev"
			}

			return &VersionCommand{
				Revision:          GitCommit,
				Version:           ver,
				VersionPrerelease: rel,
				Ui:                ui,
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
