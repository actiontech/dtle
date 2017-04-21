package command

import (
	"os"

	"github.com/mitchellh/cli"

	"udup/agent"
)

// The git commit that was compiled. This will be filled in by the compiler.
var (
	Version   string
	GitBranch string
	GitCommit string
)

// Commands is the mapping of all the available Serf commands.
var Commands map[string]cli.CommandFactory

func init() {
	meta := Meta{}
	if meta.Ui == nil {
		meta.Ui = &cli.BasicUi{
			Reader:      os.Stdin,
			Writer:      os.Stdout,
			ErrorWriter: os.Stderr,
		}
	}

	Commands = map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &agent.Command{
				Version:    Version,
				Ui:         meta.Ui,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
		"alloc-status": func() (cli.Command, error) {
			return &AllocStatusCommand{
				Meta: meta,
			}, nil
		},
		"agent-info": func() (cli.Command, error) {
			return &AgentInfoCommand{
				Meta: meta,
			}, nil
		},
		"client-config": func() (cli.Command, error) {
			return &ClientConfigCommand{
				Meta: meta,
			}, nil
		},
		"eval-status": func() (cli.Command, error) {
			return &EvalStatusCommand{
				Meta: meta,
			}, nil
		},
		"node-status": func() (cli.Command, error) {
			return &NodeStatusCommand{
				Meta: meta,
			}, nil
		},
		"run": func() (cli.Command, error) {
			return &RunCommand{
				Meta: meta,
			}, nil
		},
		"stop": func() (cli.Command, error) {
			return &StopCommand{
				Meta: meta,
			}, nil
		},
		"server-force-leave": func() (cli.Command, error) {
			return &ServerForceLeaveCommand{
				Meta: meta,
			}, nil
		},
		"server-join": func() (cli.Command, error) {
			return &ServerJoinCommand{
				Meta: meta,
			}, nil
		},
		"server-members": func() (cli.Command, error) {
			return &ServerMembersCommand{
				Meta: meta,
			}, nil
		},
		"status": func() (cli.Command, error) {
			return &StatusCommand{
				Meta: meta,
			}, nil
		},
		"validate": func() (cli.Command, error) {
			return &ValidateCommand{
				Meta: meta,
			}, nil
		},
		"version": func() (cli.Command, error) {
			return &VersionCommand{
				Version: Version,
				Commit:  GitCommit,
				Branch:  GitBranch,
				Ui:      meta.Ui,
			}, nil
		},
	}
}
