package agent

import (
	"sync"

	uconf "udup/config"
)

type Agent struct {
	config *uconf.Config

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *uconf.Config) (*Agent, error) {
	a := &Agent{
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	return a, nil
}
