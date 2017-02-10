package inputs

type ServiceInput interface {
	// SampleConfig returns the default configuration of the Input
	SampleConfig() string

	// Description returns a one-sentence description on the Input
	Description() string

	// Start starts the ServiceInput's service, whatever that may be
	Start() error

	// Stop stops the services and closes any necessary channels and connections
	Stop()
}
