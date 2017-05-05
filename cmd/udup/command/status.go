package command

import (
	"fmt"
	"strings"
	"sort"

	"udup/api"
)

const (
	// maxFailedTGs is the maximum number of tasks we show failure reasons
	// for before defering to eval-status
	maxFailedTGs = 5
)

type StatusCommand struct {
	Meta
	length    int
	evals     bool
	allAllocs bool
	verbose   bool
}

func (c *StatusCommand) Help() string {
	helpText := `
Usage: server status [options] <job>

  Display status information about jobs. If no job ID is given,
  a list of all known jobs will be dumped.

General Options:

  ` + generalOptionsUsage() + `

Status Options:

  -short
    Display short output. Used only when a single job is being
    queried, and drops verbose information about allocations.

  -evals
    Display the evaluations associated with the job.

  -all-allocs
    Display all allocations matching the job ID, including those from an older
    instance of the job.

  -verbose
    Display full information.
`
	return strings.TrimSpace(helpText)
}

func (c *StatusCommand) Synopsis() string {
	return "Display status information about jobs"
}

func (c *StatusCommand) Run(args []string) int {
	var short bool

	flags := c.Meta.FlagSet("status", FlagSetClient)
	flags.Usage = func() { c.Ui.Output(c.Help()) }
	flags.BoolVar(&short, "short", false, "")
	flags.BoolVar(&c.evals, "evals", false, "")
	flags.BoolVar(&c.allAllocs, "all-allocs", false, "")
	flags.BoolVar(&c.verbose, "verbose", false, "")

	if err := flags.Parse(args); err != nil {
		return 1
	}

	// Check that we either got no jobs or exactly one.
	args = flags.Args()
	if len(args) > 1 {
		c.Ui.Error(c.Help())
		return 1
	}

	// Truncate the id unless full length is requested
	c.length = shortId
	if c.verbose {
		c.length = fullId
	}

	// Get the HTTP client
	client, err := c.Meta.Client()
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error initializing client: %s", err))
		return 1
	}

	// Invoke list mode if no job ID.
	if len(args) == 0 {
		jobs, _, err := client.Jobs().List(nil)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error querying jobs: %s", err))
			return 1
		}

		if len(jobs) == 0 {
			// No output if we have no jobs
			c.Ui.Output("No running jobs")
		} else {
			c.Ui.Output(createStatusListOutput(jobs))
		}
		return 0
	}

	// Try querying the job
	jobID := args[0]
	jobs, _, err := client.Jobs().PrefixList(jobID)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying job: %s", err))
		return 1
	}
	if len(jobs) == 0 {
		c.Ui.Error(fmt.Sprintf("No job(s) with prefix or id %q found", jobID))
		return 1
	}
	if len(jobs) > 1 && strings.TrimSpace(jobID) != jobs[0].ID {
		c.Ui.Output(fmt.Sprintf("Prefix matched multiple jobs\n\n%s", createStatusListOutput(jobs)))
		return 0
	}
	// Prefix lookup matched a single job
	job, _, err := client.Jobs().Info(jobs[0].ID, nil)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying job: %s", err))
		return 1
	}

	// Format the job info
	basic := []string{
		fmt.Sprintf("ID|%s", *job.ID),
		fmt.Sprintf("Name|%s", *job.Name),
		fmt.Sprintf("Type|%s", *job.Type),
		fmt.Sprintf("Datacenters|%s", strings.Join(job.Datacenters, ",")),
		fmt.Sprintf("Status|%s", *job.Status),
	}

	c.Ui.Output(formatKV(basic))

	// Exit early
	if short {
		return 0
	}

	if err := c.outputJobInfo(client, job); err != nil {
		c.Ui.Error(err.Error())
		return 1
	}

	return 0
}

// outputJobInfo prints information about the passed non-periodic job. If a
// request fails, an error is returned.
func (c *StatusCommand) outputJobInfo(client *api.Client, job *api.Job) error {
	var evals, allocs []string

	// Query the allocations
	jobAllocs, _, err := client.Jobs().Allocations(*job.ID, c.allAllocs, nil)
	if err != nil {
		return fmt.Errorf("Error querying job allocations: %s", err)
	}

	// Query the evaluations
	jobEvals, _, err := client.Jobs().Evaluations(*job.ID, nil)
	if err != nil {
		return fmt.Errorf("Error querying job evaluations: %s", err)
	}

	// Output the summary
	if err := c.outputJobSummary(client, job); err != nil {
		return err
	}

	// Determine latest evaluation with failures whose follow up hasn't
	// completed, this is done while formatting
	var latestFailedPlacement *api.Evaluation
	blockedEval := false

	// Format the evals
	evals = make([]string, len(jobEvals)+1)
	evals[0] = "ID|Triggered By|Status|Placement Failures"
	for i, eval := range jobEvals {
		failures, _ := evalFailureStatus(eval)
		evals[i+1] = fmt.Sprintf("%s|%s|%s|%s",
			limit(eval.ID, c.length),
			eval.TriggeredBy,
			eval.Status,
			failures,
		)

		if eval.Status == "blocked" {
			blockedEval = true
		}

		if len(eval.FailedTGAllocs) == 0 {
			// Skip evals without failures
			continue
		}

		if latestFailedPlacement == nil || latestFailedPlacement.CreateIndex < eval.CreateIndex {
			latestFailedPlacement = eval
		}
	}

	if c.verbose || c.evals {
		c.Ui.Output(c.Colorize().Color("\n[bold]Evaluations[reset]"))
		c.Ui.Output(formatList(evals))
	}

	if blockedEval && latestFailedPlacement != nil {
		c.outputFailedPlacements(latestFailedPlacement)
	}

	// Format the allocs
	c.Ui.Output(c.Colorize().Color("\n[bold]Allocations[reset]"))
	if len(jobAllocs) > 0 {
		allocs = make([]string, len(jobAllocs)+1)
		allocs[0] = "ID|Eval ID|Node ID|Task|Desired|Status|Created At"
		for i, alloc := range jobAllocs {
			allocs[i+1] = fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
				limit(alloc.ID, c.length),
				limit(alloc.EvalID, c.length),
				limit(alloc.NodeID, c.length),
				alloc.Task,
				alloc.DesiredStatus,
				alloc.ClientStatus,
				formatUnixNanoTime(alloc.CreateTime))
		}

		c.Ui.Output(formatList(allocs))
	} else {
		c.Ui.Output("No allocations placed")
	}
	return nil
}

// outputJobSummary displays the given jobs summary and children job summary
// where appropriate
func (c *StatusCommand) outputJobSummary(client *api.Client, job *api.Job) error {
	// Query the summary
	summary, _, err := client.Jobs().Summary(*job.ID, nil)
	if err != nil {
		return fmt.Errorf("Error querying job summary: %s", err)
	}

	if summary == nil {
		return nil
	}

	c.Ui.Output(fmt.Sprintf("---summary：%v", summary))
	c.Ui.Output(c.Colorize().Color("\n[bold]Summary[reset]"))
	summaries := make([]string, len(summary.Tasks)+1)
	summaries[0] = "Task|Status"

	for idx, task := range summary.Tasks {
		summaries[idx+1] = fmt.Sprintf("%s|%s",
			task, task.Status,
		)
	}
	c.Ui.Output(formatList(summaries))

	return nil
}

func (c *StatusCommand) outputFailedPlacements(failedEval *api.Evaluation) {
	if failedEval == nil || len(failedEval.FailedTGAllocs) == 0 {
		return
	}

	c.Ui.Output(c.Colorize().Color("\n[bold]Placement Failure[reset]"))

	sorted := sortedTaskFromMetrics(failedEval.FailedTGAllocs)
	for i, tg := range sorted {
		if i >= maxFailedTGs {
			break
		}

		c.Ui.Output(fmt.Sprintf("Task %q:", tg))
		metrics := failedEval.FailedTGAllocs[tg]
		c.Ui.Output(formatAllocMetrics(metrics, false, "  "))
		if i != len(sorted)-1 {
			c.Ui.Output("")
		}
	}

	if len(sorted) > maxFailedTGs {
		trunc := fmt.Sprintf("\nPlacement failures truncated. To see remainder run:\nserver eval-status %s", failedEval.ID)
		c.Ui.Output(trunc)
	}
}

func sortedTaskFromMetrics(groups map[string]*api.AllocationMetric) []string {
	tgs := make([]string, 0, len(groups))
	for tg, _ := range groups {
		tgs = append(tgs, tg)
	}
	sort.Strings(tgs)
	return tgs
}

// list general information about a list of jobs
func createStatusListOutput(jobs []*api.JobListStub) string {
	out := make([]string, len(jobs)+1)
	out[0] = "ID|Type|Status"
	for i, job := range jobs {
		out[i+1] = fmt.Sprintf("%s|%s|%s",
			job.ID,
			job.Type,
			job.Status)
	}
	return formatList(out)
}
