package command

import (
	"strings"

	"time"

	"strconv"

	"src/github.com/rakyll/autopprof"
)

type PprofCommand struct {
	Meta
}

func (c *PprofCommand) Help() string {
	helpText := `
Usage: dtle pprof [options] <job>

  begin  start a pprof, use for Performance analysis .

  `
	return strings.TrimSpace(helpText)
}

func (c *PprofCommand) Run(args []string) int {
	flags := c.Meta.FlagSet("status", FlagSetClient)
	// Check that we got exactly one argument
	args = flags.Args()
	if len(args) != 1 {
		c.Ui.Error(c.Help())
		return 1
	}
	// Check that we got exactly one node
	args = flags.Args()
	if len(args) != 1 {
		c.Ui.Error(c.Help())
		return 1
	}
	times, err := strconv.Atoi(args[0])
	if nil != err {
		c.Ui.Error(c.Help())
		return 1
	}
	autopprof.Capture(autopprof.CPUProfile{
		Duration: time.Duration(times) * time.Second,
	})
	return 0
}

func (c *PprofCommand) Synopsis() string {
	return "Prints the Dtle version"
}
