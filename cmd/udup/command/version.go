package command

import (
	"bytes"
	"fmt"

	"github.com/mitchellh/cli"
)

// VersionCommand is a Command implementation prints the version.
type VersionCommand struct {
	Version string
	Branch  string
	Commit  string
	Ui      cli.Ui
}

func (c *VersionCommand) Run(_ []string) int {
	var versionString bytes.Buffer

	fmt.Fprintf(&versionString, "Udup %s (git: %s %s)", c.Version, c.Branch, c.Commit)
	c.Ui.Output(versionString.String())
	return 0
}

func (c *VersionCommand) Synopsis() string {
	return "Prints the Udup version"
}

func (c *VersionCommand) Help() string {
	return ""
}
