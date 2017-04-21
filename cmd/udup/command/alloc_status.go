package command

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/colorstring"

	"udup/api"
	"udup/internal/client"
)

type AllocStatusCommand struct {
	Meta
	color *colorstring.Colorize
}

func (c *AllocStatusCommand) Help() string {
	helpText := `
Usage: server alloc-status [options] <allocation>

  Display information about existing allocations and its tasks. This command can
  be used to inspect the current status of an allocation, including its running
  status, metadata, and verbose failure messages reported by internal
  subsystems.

General Options:

  ` + generalOptionsUsage() + `

Alloc Status Options:

  -short
    Display short output. Shows only the most recent task event.

  -stats
    Display detailed resource usage statistics.

  -verbose
    Show full information.

  -json
    Output the allocation in its JSON format.

  -t
    Format and display allocation using a Go template.
`

	return strings.TrimSpace(helpText)
}

func (c *AllocStatusCommand) Synopsis() string {
	return "Display allocation status information and metadata"
}

func (c *AllocStatusCommand) Run(args []string) int {
	var short, displayStats, verbose, json bool
	var tmpl string

	flags := c.Meta.FlagSet("alloc-status", FlagSetClient)
	flags.Usage = func() { c.Ui.Output(c.Help()) }
	flags.BoolVar(&short, "short", false, "")
	flags.BoolVar(&verbose, "verbose", false, "")
	flags.BoolVar(&displayStats, "stats", false, "")
	flags.BoolVar(&json, "json", false, "")
	flags.StringVar(&tmpl, "t", "", "")

	if err := flags.Parse(args); err != nil {
		return 1
	}

	// Check that we got exactly one allocation ID
	args = flags.Args()

	// Get the HTTP client
	client, err := c.Meta.Client()
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error initializing client: %s", err))
		return 1
	}

	// If args not specified but output format is specified, format and output the allocations data list
	if len(args) == 0 {
		var format string
		if json && len(tmpl) > 0 {
			c.Ui.Error("Both -json and -t are not allowed")
			return 1
		} else if json {
			format = "json"
		} else if len(tmpl) > 0 {
			format = "template"
		}
		if len(format) > 0 {
			allocs, _, err := client.Allocations().List(nil)
			if err != nil {
				c.Ui.Error(fmt.Sprintf("Error querying allocations: %v", err))
				return 1
			}
			// Return nothing if no allocations found
			if len(allocs) == 0 {
				return 0
			}

			f, err := DataFormat(format, tmpl)
			if err != nil {
				c.Ui.Error(fmt.Sprintf("Error getting formatter: %s", err))
				return 1
			}

			out, err := f.TransformData(allocs)
			if err != nil {
				c.Ui.Error(fmt.Sprintf("Error formatting the data: %s", err))
				return 1
			}
			c.Ui.Output(out)
			return 0
		}
	}

	if len(args) != 1 {
		c.Ui.Error(c.Help())
		return 1
	}
	allocID := args[0]

	// Truncate the id unless full length is requested
	length := shortId
	if verbose {
		length = fullId
	}

	// Query the allocation info
	if len(allocID) == 1 {
		c.Ui.Error(fmt.Sprintf("Identifier must contain at least two characters."))
		return 1
	}
	if len(allocID)%2 == 1 {
		// Identifiers must be of even length, so we strip off the last byte
		// to provide a consistent user experience.
		allocID = allocID[:len(allocID)-1]
	}

	allocs, _, err := client.Allocations().PrefixList(allocID)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying allocation: %v", err))
		return 1
	}
	if len(allocs) == 0 {
		c.Ui.Error(fmt.Sprintf("No allocation(s) with prefix or id %q found", allocID))
		return 1
	}
	if len(allocs) > 1 {
		// Format the allocs
		out := make([]string, len(allocs)+1)
		out[0] = "ID|Eval ID|Job ID|Task|Desired Status|Client Status"
		for i, alloc := range allocs {
			out[i+1] = fmt.Sprintf("%s|%s|%s|%s|%s|%s",
				limit(alloc.ID, length),
				limit(alloc.EvalID, length),
				alloc.JobID,
				alloc.Task,
				alloc.DesiredStatus,
				alloc.ClientStatus,
			)
		}
		c.Ui.Output(fmt.Sprintf("Prefix matched multiple allocations\n\n%s", formatList(out)))
		return 0
	}
	// Prefix lookup matched a single allocation
	alloc, _, err := client.Allocations().Info(allocs[0].ID, nil)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying allocation: %s", err))
		return 1
	}

	// If output format is specified, format and output the data
	var format string
	if json && len(tmpl) > 0 {
		c.Ui.Error("Both -json and -t are not allowed")
		return 1
	} else if json {
		format = "json"
	} else if len(tmpl) > 0 {
		format = "template"
	}
	if len(format) > 0 {
		f, err := DataFormat(format, tmpl)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error getting formatter: %s", err))
			return 1
		}

		out, err := f.TransformData(alloc)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error formatting the data: %s", err))
			return 1
		}
		c.Ui.Output(out)
		return 0
	}

	// Format the allocation data
	basic := []string{
		fmt.Sprintf("ID|%s", limit(alloc.ID, length)),
		fmt.Sprintf("Eval ID|%s", limit(alloc.EvalID, length)),
		fmt.Sprintf("Name|%s", alloc.Name),
		fmt.Sprintf("Node ID|%s", limit(alloc.NodeID, length)),
		fmt.Sprintf("Job ID|%s", alloc.JobID),
		fmt.Sprintf("Client Status|%s", alloc.ClientStatus),
		fmt.Sprintf("Client Description|%s", alloc.ClientDescription),
		fmt.Sprintf("Desired Status|%s", alloc.DesiredStatus),
		fmt.Sprintf("Desired Description|%s", alloc.DesiredDescription),
		fmt.Sprintf("Created At|%s", formatUnixNanoTime(alloc.CreateTime)),
	}

	if verbose {
		basic = append(basic,
			fmt.Sprintf("Evaluated Nodes|%d", alloc.Metrics.NodesEvaluated),
			fmt.Sprintf("Filtered Nodes|%d", alloc.Metrics.NodesFiltered),
			fmt.Sprintf("Exhausted Nodes|%d", alloc.Metrics.NodesExhausted),
			fmt.Sprintf("Allocation Time|%s", alloc.Metrics.AllocationTime),
			fmt.Sprintf("Failures|%d", alloc.Metrics.CoalescedFailures))
	}
	c.Ui.Output(formatKV(basic))

	if short {
		c.shortTaskStatus(alloc)
	} else {
		var statsErr error
		var stats *api.AllocStatistics
		stats, statsErr = client.Allocations().Stats(alloc, nil)
		if statsErr != nil {
			c.Ui.Output("")
			if statsErr != api.NodeDownErr {
				c.Ui.Error(fmt.Sprintf("Couldn't retrieve stats (HINT: ensure Client.Advertise.HTTP is set): %v", statsErr))
			} else {
				c.Ui.Output("Omitting resource statistics since the node is down.")
			}
		}
		c.outputTaskDetails(alloc, stats, displayStats)
	}

	// Format the detailed status
	if verbose {
		c.Ui.Output(c.Colorize().Color("\n[bold]Placement Metrics[reset]"))
		c.Ui.Output(formatAllocMetrics(alloc.Metrics, true, "  "))
	}

	return 0
}

// outputTaskDetails prints task details for each task in the allocation,
// optionally printing verbose statistics if displayStats is set
func (c *AllocStatusCommand) outputTaskDetails(alloc *api.Allocation, stats *api.AllocStatistics, displayStats bool) {
	for task := range c.sortedTaskStateIterator(alloc.TaskStates) {
		state := alloc.TaskStates[task]
		c.Ui.Output(c.Colorize().Color(fmt.Sprintf("\n[bold]Task %q is %q[reset]", task, state.State)))
		c.Ui.Output("")
		c.outputTaskStatus(state)
	}
}

// outputTaskStatus prints out a list of the most recent events for the given
// task state.
func (c *AllocStatusCommand) outputTaskStatus(state *api.TaskState) {
	c.Ui.Output("Recent Events:")
	events := make([]string, len(state.Events)+1)
	events[0] = "Time|Type|Description"

	size := len(state.Events)
	for i, event := range state.Events {
		formatedTime := formatUnixNanoTime(event.Time)

		// Build up the description based on the event type.
		var desc string
		switch event.Type {
		case api.TaskSetup:
			desc = event.Message
		case api.TaskStarted:
			desc = "Task started by client"
		case api.TaskReceived:
			desc = "Task received by client"
		case api.TaskFailedValidation:
			if event.ValidationError != "" {
				desc = event.ValidationError
			} else {
				desc = "Validation of task failed"
			}
		case api.TaskSetupFailure:
			if event.SetupError != "" {
				desc = event.SetupError
			} else {
				desc = "Task setup failed"
			}
		case api.TaskDriverFailure:
			if event.DriverError != "" {
				desc = event.DriverError
			} else {
				desc = "Failed to start task"
			}
		case api.TaskKilling:
			if event.KillReason != "" {
				desc = fmt.Sprintf("Killing task: %v", event.KillReason)
			} else if event.KillTimeout != 0 {
				desc = fmt.Sprintf("Sent interrupt. Waiting %v before force killing", event.KillTimeout)
			} else {
				desc = "Sent interrupt"
			}
		case api.TaskKilled:
			if event.KillError != "" {
				desc = event.KillError
			} else {
				desc = "Task successfully killed"
			}
		case api.TaskTerminated:
			var parts []string
			parts = append(parts, fmt.Sprintf("Exit Code: %d", event.ExitCode))

			if event.Signal != 0 {
				parts = append(parts, fmt.Sprintf("Signal: %d", event.Signal))
			}

			if event.Message != "" {
				parts = append(parts, fmt.Sprintf("Exit Message: %q", event.Message))
			}
			desc = strings.Join(parts, ", ")
		case api.TaskRestarting:
			in := fmt.Sprintf("Task restarting in %v", time.Duration(event.StartDelay))
			if event.RestartReason != "" && event.RestartReason != client.ReasonWithinPolicy {
				desc = fmt.Sprintf("%s - %s", event.RestartReason, in)
			} else {
				desc = in
			}
		case api.TaskNotRestarting:
			if event.RestartReason != "" {
				desc = event.RestartReason
			} else {
				desc = "Task exceeded restart policy"
			}
		case api.TaskSiblingFailed:
			if event.FailedSibling != "" {
				desc = fmt.Sprintf("Task's sibling %q failed", event.FailedSibling)
			} else {
				desc = "Task's sibling failed"
			}
		case api.TaskRestartSignal:
			if event.RestartReason != "" {
				desc = event.RestartReason
			} else {
				desc = "Task signaled to restart"
			}
		case api.TaskDriverMessage:
			desc = event.DriverMessage
		case api.TaskLeaderDead:
			desc = "Leader Task in Group dead"
		}

		// Reverse order so we are sorted by time
		events[size-i] = fmt.Sprintf("%s|%s|%s", formatedTime, event.Type, desc)
	}
	c.Ui.Output(formatList(events))
}

// shortTaskStatus prints out the current state of each task.
func (c *AllocStatusCommand) shortTaskStatus(alloc *api.Allocation) {
	tasks := make([]string, 0, len(alloc.TaskStates)+1)
	tasks = append(tasks, "Name|State|Last Event|Time")
	for task := range c.sortedTaskStateIterator(alloc.TaskStates) {
		state := alloc.TaskStates[task]
		lastState := state.State
		var lastEvent, lastTime string

		l := len(state.Events)
		if l != 0 {
			last := state.Events[l-1]
			lastEvent = last.Type
			lastTime = formatUnixNanoTime(last.Time)
		}

		tasks = append(tasks, fmt.Sprintf("%s|%s|%s|%s",
			task, lastState, lastEvent, lastTime))
	}

	c.Ui.Output(c.Colorize().Color("\n[bold]Tasks[reset]"))
	c.Ui.Output(formatList(tasks))
}

// sortedTaskStateIterator is a helper that takes the task state map and returns a
// channel that returns the keys in a sorted order.
func (c *AllocStatusCommand) sortedTaskStateIterator(m map[string]*api.TaskState) <-chan string {
	output := make(chan string, len(m))
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	for _, key := range keys {
		output <- key
	}

	close(output)
	return output
}
