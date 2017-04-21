package command

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mitchellh/colorstring"

	"udup/api"
)

const (
	// floatFormat is a format string for formatting floats.
	floatFormat = "#,###.##"

	// bytesPerMegabyte is the number of bytes per MB
	bytesPerMegabyte = 1024 * 1024
)

type NodeStatusCommand struct {
	Meta
	color       *colorstring.Colorize
	length      int
	verbose     bool
	list_allocs bool
	self        bool
	stats       bool
	json        bool
	tmpl        string
}

func (c *NodeStatusCommand) Help() string {
	helpText := `
Usage: server node-status [options] <node>

  Display status information about a given node. The list of nodes
  returned includes only nodes which jobs may be scheduled to, and
  includes status and other high-level information.

  If a node ID is passed, information for that specific node will be displayed,
  including resource usage statistics. If no node ID's are passed, then a
  short-hand list of all nodes will be displayed. The -self flag is useful to
  quickly access the status of the local node.

General Options:

  ` + generalOptionsUsage() + `

Node Status Options:

  -self
    Query the status of the local node.

  -stats 
    Display detailed resource usage statistics.

  -allocs
    Display a count of running allocations for each node.

  -verbose
    Display full information.

  -json
    Output the node in its JSON format.

  -t
    Format and display node using a Go template.
`
	return strings.TrimSpace(helpText)
}

func (c *NodeStatusCommand) Synopsis() string {
	return "Display status information about nodes"
}

func (c *NodeStatusCommand) Run(args []string) int {

	flags := c.Meta.FlagSet("node-status", FlagSetClient)
	flags.Usage = func() { c.Ui.Output(c.Help()) }
	flags.BoolVar(&c.verbose, "verbose", false, "")
	flags.BoolVar(&c.list_allocs, "allocs", false, "")
	flags.BoolVar(&c.self, "self", false, "")
	flags.BoolVar(&c.stats, "stats", false, "")
	flags.BoolVar(&c.json, "json", false, "")
	flags.StringVar(&c.tmpl, "t", "", "")

	if err := flags.Parse(args); err != nil {
		return 1
	}

	// Check that we got either a single node or none
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

	// Use list mode if no node name was provided
	if len(args) == 0 && !c.self {
		// If output format is specified, format and output the node data list
		var format string
		if c.json && len(c.tmpl) > 0 {
			c.Ui.Error("Both -json and -t are not allowed")
			return 1
		} else if c.json {
			format = "json"
		} else if len(c.tmpl) > 0 {
			format = "template"
		}

		// Query the node info
		nodes, _, err := client.Nodes().List(nil)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error querying node status: %s", err))
			return 1
		}

		// Return nothing if no nodes found
		if len(nodes) == 0 {
			return 0
		}

		if len(format) > 0 {
			f, err := DataFormat(format, c.tmpl)
			if err != nil {
				c.Ui.Error(fmt.Sprintf("Error getting formatter: %s", err))
				return 1
			}

			out, err := f.TransformData(nodes)
			if err != nil {
				c.Ui.Error(fmt.Sprintf("Error formatting the data: %s", err))
				return 1
			}
			c.Ui.Output(out)
			return 0
		}

		// Format the nodes list
		out := make([]string, len(nodes)+1)
		if c.list_allocs {
			out[0] = "ID|DC|Name|Status|Running Allocs"
		} else {
			out[0] = "ID|DC|Name|Status"
		}

		for i, node := range nodes {
			if c.list_allocs {
				numAllocs, err := getRunningAllocs(client, node.ID)
				if err != nil {
					c.Ui.Error(fmt.Sprintf("Error querying node allocations: %s", err))
					return 1
				}
				out[i+1] = fmt.Sprintf("%s|%s|%s|%s|%v",
					limit(node.ID, c.length),
					node.Datacenter,
					node.Name,
					node.Status,
					len(numAllocs))
			} else {
				out[i+1] = fmt.Sprintf("%s|%s|%s|%s",
					limit(node.ID, c.length),
					node.Datacenter,
					node.Name,
					node.Status)
			}
		}

		// Dump the output
		c.Ui.Output(formatList(out))
		return 0
	}

	// Query the specific node
	nodeID := ""
	if !c.self {
		nodeID = args[0]
	} else {
		var err error
		if nodeID, err = getLocalNodeID(client); err != nil {
			c.Ui.Error(err.Error())
			return 1
		}
	}
	if len(nodeID) == 1 {
		c.Ui.Error(fmt.Sprintf("Identifier must contain at least two characters."))
		return 1
	}
	if len(nodeID)%2 == 1 {
		// Identifiers must be of even length, so we strip off the last byte
		// to provide a consistent user experience.
		nodeID = nodeID[:len(nodeID)-1]
	}

	nodes, _, err := client.Nodes().PrefixList(nodeID)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying node info: %s", err))
		return 1
	}
	// Return error if no nodes are found
	if len(nodes) == 0 {
		c.Ui.Error(fmt.Sprintf("No node(s) with prefix %q found", nodeID))
		return 1
	}
	if len(nodes) > 1 {
		// Format the nodes list that matches the prefix so that the user
		// can create a more specific request
		out := make([]string, len(nodes)+1)
		out[0] = "ID|DC|Name|Status"
		for i, node := range nodes {
			out[i+1] = fmt.Sprintf("%s|%s|%s|%s",
				limit(node.ID, c.length),
				node.Datacenter,
				node.Name,
				node.Status)
		}
		// Dump the output
		c.Ui.Output(fmt.Sprintf("Prefix matched multiple nodes\n\n%s", formatList(out)))
		return 0
	}
	// Prefix lookup matched a single node
	node, _, err := client.Nodes().Info(nodes[0].ID, nil)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying node info: %s", err))
		return 1
	}

	// If output format is specified, format and output the data
	var format string
	if c.json && len(c.tmpl) > 0 {
		c.Ui.Error("Both -json and -t are not allowed")
		return 1
	} else if c.json {
		format = "json"
	} else if len(c.tmpl) > 0 {
		format = "template"
	}
	if len(format) > 0 {
		f, err := DataFormat(format, c.tmpl)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error getting formatter: %s", err))
			return 1
		}

		out, err := f.TransformData(node)
		if err != nil {
			c.Ui.Error(fmt.Sprintf("Error formatting the data: %s", err))
			return 1
		}
		c.Ui.Output(out)
		return 0
	}

	return c.formatNode(client, node)
}

func nodeDrivers(n *api.Node) []string {
	var drivers []string
	for k, v := range n.Attributes {
		// driver.docker = 1
		parts := strings.Split(k, ".")
		if len(parts) != 2 {
			continue
		} else if parts[0] != "driver" {
			continue
		} else if v != "1" {
			continue
		}

		drivers = append(drivers, parts[1])
	}

	sort.Strings(drivers)
	return drivers
}

func (c *NodeStatusCommand) formatNode(client *api.Client, node *api.Node) int {
	// Format the header output
	basic := []string{
		fmt.Sprintf("ID|%s", limit(node.ID, c.length)),
		fmt.Sprintf("Name|%s", node.Name),
		fmt.Sprintf("DC|%s", node.Datacenter),
		fmt.Sprintf("Status|%s", node.Status),
		fmt.Sprintf("Drivers|%s", strings.Join(nodeDrivers(node), ",")),
	}

	c.Ui.Output(c.Colorize().Color(formatKV(basic)))

	allocs, err := getAllocs(client, node, c.length)
	if err != nil {
		c.Ui.Error(fmt.Sprintf("Error querying node allocations: %s", err))
		return 1
	}

	if len(allocs) > 1 {
		c.Ui.Output(c.Colorize().Color("\n[bold]Allocations[reset]"))
		c.Ui.Output(formatList(allocs))
	}

	if c.verbose {
		c.formatAttributes(node)
		c.formatMeta(node)
	}
	return 0

}

func (c *NodeStatusCommand) formatAttributes(node *api.Node) {
	// Print the attributes
	keys := make([]string, len(node.Attributes))
	for k := range node.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var attributes []string
	for _, k := range keys {
		if k != "" {
			attributes = append(attributes, fmt.Sprintf("%s|%s", k, node.Attributes[k]))
		}
	}
	c.Ui.Output(c.Colorize().Color("\n[bold]Attributes[reset]"))
	c.Ui.Output(formatKV(attributes))
}

func (c *NodeStatusCommand) formatMeta(node *api.Node) {
	// Print the meta
	keys := make([]string, 0, len(node.Meta))
	for k := range node.Meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var meta []string
	for _, k := range keys {
		if k != "" {
			meta = append(meta, fmt.Sprintf("%s|%s", k, node.Meta[k]))
		}
	}
	c.Ui.Output(c.Colorize().Color("\n[bold]Meta[reset]"))
	c.Ui.Output(formatKV(meta))
}

// getRunningAllocs returns a slice of allocation id's running on the node
func getRunningAllocs(client *api.Client, nodeID string) ([]*api.Allocation, error) {
	var allocs []*api.Allocation

	// Query the node allocations
	nodeAllocs, _, err := client.Nodes().Allocations(nodeID, nil)
	// Filter list to only running allocations
	for _, alloc := range nodeAllocs {
		if alloc.ClientStatus == "running" {
			allocs = append(allocs, alloc)
		}
	}
	return allocs, err
}

// getAllocs returns information about every running allocation on the node
func getAllocs(client *api.Client, node *api.Node, length int) ([]string, error) {
	var allocs []string
	// Query the node allocations
	nodeAllocs, _, err := client.Nodes().Allocations(node.ID, nil)
	// Format the allocations
	allocs = make([]string, len(nodeAllocs)+1)
	allocs[0] = "ID|Eval ID|Job ID|Task|Desired Status|Client Status"
	for i, alloc := range nodeAllocs {
		allocs[i+1] = fmt.Sprintf("%s|%s|%s|%s|%s|%s",
			limit(alloc.ID, length),
			limit(alloc.EvalID, length),
			alloc.JobID,
			alloc.Task,
			alloc.DesiredStatus,
			alloc.ClientStatus)
	}
	return allocs, err
}
