package agent

// processor plugins must implement this interface
type Processor interface {
	// Main plugin method, will be called when an execution is done.
	Process(args *ProcessorArgs) Job
}

// Arguments for calling an execution processor
type ProcessorArgs struct {
	// The job to pass to the processor
	Job Job
	// The configuration for this plugin call
	Config PluginConfig
}

// Represents a plgin config data structure
type PluginConfig map[string]interface{}
