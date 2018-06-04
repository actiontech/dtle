package command

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"

	"udup/agent"
	"udup/api"
)

var (
	// enforceIndexRegex is a regular expression which extracts the enforcement error
	enforceIndexRegex = regexp.MustCompile(`\((Enforcing job modify index.*)\)`)
)

type StartCommand struct {
	Meta
	JobGetter
}

func (c *StartCommand) Help() string {
	helpText := `
Usage: udup start [options] <path>

  Starts running a new job or updates an existing job using
  the specification located at <path>. This is the main command
  used to interact with Udup.

  If the supplied path is "-", the jobfile is read from stdin. Otherwise
  it is read from the file at the supplied path or downloaded and
  read from URL specified.

  Upon successful job submission, this command will immediately
  enter an interactive monitor. This is useful to watch Udup's
  internals make scheduling decisions and place the submitted work
  onto nodes. The monitor will end once job placement is done. It
  is safe to exit the monitor early using ctrl+c.

  On successful job submission and scheduling, exit code 0 will be
  returned. If there are job placement issues encountered
  (unsatisfiable constraints, resource exhaustion, etc), then the
  exit code will be 2. Any other errors, including client connection
  issues or internal errors, are indicated by exit code 1.

  If the job has specified the region, the -region flag and UDUP_REGION
  environment variable are overridden and the job's region is used.

General Options:

  ` + generalOptionsUsage() + `

Run Options:

  -check-index
    If set, the job is only registered or updated if the passed
    job modify index matches the server side version. If a check-index value of
    zero is passed, the job is only registered if it does not yet exist. If a
    non-zero value is passed, it ensures that the job is being updated from a
    known state. The use of this flag is most common in conjunction with plan
    command.

  -detach
    Return immediately instead of entering monitor mode. After job submission,
    the evaluation ID will be printed to the screen, which can be used to
    examine the evaluation using the eval-status command.

  -verbose
    Display full information.

  -output
    Output the JSON that would be submitted to the HTTP API without submitting
    the job.
`
	return strings.TrimSpace(helpText)
}

func (c *StartCommand) Synopsis() string {
	return "Run a new job or update an existing job"
}

func (c *StartCommand) Run(args []string) int {
	var detach, verbose, output bool
	var checkIndexStr string

	flags := c.Meta.FlagSet("start", FlagSetClient)
	flags.Usage = func() { c.Ui.Output(c.Help()) }
	flags.BoolVar(&detach, "detach", false, "")
	flags.BoolVar(&verbose, "verbose", false, "")
	flags.BoolVar(&output, "output", false, "")
	flags.StringVar(&checkIndexStr, "check-index", "", "")

	if err := flags.Parse(args); err != nil {
		return 1
	}

	length := fullId

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

	// Get Job struct from Jobfile
	job, err := c.JobGetter.ApiJob(args[0])
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error getting job struct: %s", err))
		return 1
	}

	// Get the HTTP client
	client, err := c.Meta.Client()
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error initializing client: %s", err))
		return 1
	}

	// Force the region to be that of the job.
	if r := job.Region; r != nil {
		client.SetRegion(*r)
	}

	// Check that the job is valid
	/*jr, _, err := client.Jobs().Validate(job, nil)
	if err != nil {
		jr, err = c.validateLocal(job)
	}
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error validating job: %s", err))
		return 1
	}

	if jr != nil && !jr.DriverConfigValidated {
		c.Ui.Output(
			c.Colorize().Color("[bold][yellow]Driver configuration not validated since connection to Udup agent couldn't be established.[reset]\n"))
	}

	if jr != nil && jr.Error != "" {
		c.Ui.Error(
			c.Colorize().Color("[bold][red]Job validation errors:[reset]"))
		c.Ui.Error(jr.Error)
		return 1
	}*/

	if output {
		req := api.RegisterJobRequest{Job: job}
		buf, err := json.MarshalIndent(req, "", "    ")
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error converting job: %s", err))
			return 1
		}

		c.Ui.Output(string(buf))
		return 0
	}

	// Parse the check-index
	checkIndex, enforce, err := parseCheckIndex(checkIndexStr)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error parsing check-index value %q: %v", checkIndexStr, err))
		return 1
	}

	// Submit the job
	var evalID string
	if enforce {
		evalID, _, err = client.Jobs().EnforceRegister(job, checkIndex, nil)
	} else {
		evalID, _, err = client.Jobs().Register(job, nil)
	}
	if err != nil {
		if strings.Contains(err.Error(), api.RegisterEnforceIndexErrPrefix) {
			// Format the error specially if the error is due to index
			// enforcement
			matches := enforceIndexRegex.FindStringSubmatch(err.Error())
			if len(matches) == 2 {
				c.Ui.Error(matches[1]) // The matched group
				c.Ui.Error("Job not updated")
				return 1
			}
		}

		c.Ui.Error(fmt.Sprintf("Error submitting job: %s", err))
		return 1
	}

	// Check if we should enter monitor mode
	if detach {
		c.Ui.Output("Job registration successful")
		c.Ui.Output("Evaluation ID: " + evalID)

		return 0
	}

	// Detach was not specified, so start monitoring
	mon := newMonitor(c.Ui, client, length)
	return mon.monitor(evalID, false)

}

// parseCheckIndex parses the check-index flag and returns the index, whether it
// was set and potentially an error during parsing.
func parseCheckIndex(input string) (uint64, bool, error) {
	if input == "" {
		return 0, false, nil
	}

	u, err := strconv.ParseUint(input, 10, 64)
	return u, true, err
}

// validateLocal validates without talking to a Udup agent
func (c *StartCommand) validateLocal(aj *api.Job) (*api.JobValidateResponse, error) {
	var out api.JobValidateResponse

	job := agent.ApiJobToStructJob(aj, 0)
	job.Canonicalize()

	if vErr := job.Validate(); vErr != nil {
		if merr, ok := vErr.(*multierror.Error); ok {
			for _, err := range merr.Errors {
				out.ValidationErrors = append(out.ValidationErrors, err.Error())
			}
			out.Error = merr.Error()
		} else {
			out.ValidationErrors = append(out.ValidationErrors, vErr.Error())
			out.Error = vErr.Error()
		}
	}

	return &out, nil
}
