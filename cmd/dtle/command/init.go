/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const (
	// DefaultInitName is the default name we use when
	// initializing the example file
	DefaultInitName = "example.conf"
)

// InitCommand generates a new job template that you can customize to your
// liking, like vagrant init
type InitCommand struct {
	Meta
}

func (c *InitCommand) Help() string {
	helpText := `
Usage: dts init

  Creates an example job file that can be used as a starting
  point to customize further.
`
	return strings.TrimSpace(helpText)
}

func (c *InitCommand) Synopsis() string {
	return "Create an example job file"
}

func (c *InitCommand) Run(args []string) int {
	// Check for misuse
	if len(args) != 0 {
		c.Ui.Error(c.Help())
		return 1
	}

	// Check if the file already exists
	_, err := os.Stat(DefaultInitName)
	if err != nil && !os.IsNotExist(err) {
		c.Ui.Error(fmt.Sprintf("Failed to stat '%s': %v", DefaultInitName, err))
		return 1
	}
	if !os.IsNotExist(err) {
		c.Ui.Error(fmt.Sprintf("Job '%s' already exists", DefaultInitName))
		return 1
	}

	// Write out the example
	err = ioutil.WriteFile(DefaultInitName, []byte(defaultJob), 0660)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Failed to write '%s': %v", DefaultInitName, err))
		return 1
	}

	// Success
	c.Ui.Output(fmt.Sprintf("Example job file written to %s", DefaultInitName))
	return 0
}

var defaultJob = strings.TrimSpace(`
# There can only be a single job definition per file. This job is named
# "example" so it will create a job with the ID and Name "example".

# The "job" stanza is the top-most configuration option in the job
# specification. A job is a declarative specification of tasks that Dtle
# should run. Jobs have a globally unique name, one or many tasks, which
# are themselves collections of one or many tasks.
#
job "example" {
  # The "region" parameter specifies the region in which to execute the job. If
  # omitted, this inherits the default region name of "global".
  # region = "global"

  # The "datacenters" parameter specifies the list of datacenters which should
  # be considered when placing this task.
  # datacenters = ["dc1"]

  # The "type" parameter controls the type of job, which impacts the scheduler's
  # decision on placement. This configuration is optional and defaults to
  # "synchronous". For a full list of job types and their differences, please see
  # the online documentation.
  #
  type = "synchronous"

  # The "tasks" stanza defines a task that should be co-located on
  # the same Dtle client.
  #
  task "Src" {
    node_id = "1eda45f8-df9b-1541-9009-83952e7b672a"
    # The "driver" parameter specifies the task driver that should be used to
    # run the task.
    driver = "MySQL"

    # The "config" stanza specifies the driver configuration, which is passed
    # directly to the driver to start the task. The details of configurations
    # are specific to each driver, so please see specific driver
    # documentation for more information.
    config {
      nats_addr = "127.0.0.1:8193"
      replicate_do_db = [
        {
          schema = ""
          table = ""
        },
        {
          schema = ""
          table = ""
        }
      ]
      conn {
        host = "127.0.0.1"
        port = 3306
        user = "repluser"
        password = "replpwd"
      }
    }
  }

  task "Dest" {
    node_id = "1eda45f8-df9b-1541-9009-83952e7b672a"
    # The "driver" parameter specifies the task driver that should be used to
    # run the task.
    driver = "MySQL"

    # The "config" stanza specifies the driver configuration, which is passed
    # directly to the driver to start the task. The details of configurations
    # are specific to each driver, so please see specific driver
    # documentation for more information.
    config {
      nats_addr = "127.0.0.1:8193"
      replicate_do_db = [
        {
          schema = ""
          table = ""
        },
        {
          schema = ""
          table = ""
        }
      ]
      conn {
        host = "127.0.0.1"
        port = 3306
        user = "repluser"
        password = "replpwd"
      }
    }
  }
}
`)
