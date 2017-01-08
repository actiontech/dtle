package agent

import (
	"fmt"
	"sync"

	"github.com/ngaut/log"

	uconf "udup/config"
)

type Agent struct {
	config    *uconf.Config
	extractor *Extractor
	applier   *Applier

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

	go a.listenOnPanicAbort()

	if err := a.setupApply(); err != nil {
		return nil, err
	}

	if err := a.setupExtract(); err != nil {
		return nil, err
	}

	if a.extractor == nil && a.applier == nil {
		return nil, fmt.Errorf("must have at least extract or apply mode enabled")
	}

	return a, nil
}

// setupApply is used to setup the applier if enabled
func (a *Agent) setupApply() error {
	if !a.config.Apply.Enabled {
		return nil
	}

	// Create the applier
	a.applier = NewApplier(a.config)
	go func() {
		err := a.applier.initiateApplier()
		if err != nil {
			log.Errorf("applier setup failed: %v", err)
			a.config.PanicAbort <- err
		}
	}()

	return nil
}

// setupExtract is used to setup the extractor if enabled
func (a *Agent) setupExtract() error {
	if !a.config.Extract.Enabled {
		return nil
	}

	// Create the extractor
	a.extractor = NewExtractor(a.config)
	go func() {
		err := a.extractor.initiateExtractor()
		if err != nil {
			log.Errorf("extractor setup failed: %v", err)
			a.config.PanicAbort <- err
		}

	}()
	return nil
}

// listenOnPanicAbort aborts on abort request
func (a *Agent) listenOnPanicAbort() {
	err := <-a.config.PanicAbort
	log.Errorf("agent setup failed: %v", err)
}

// Shutdown is used to terminate the agent.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	log.Infof("[INFO] agent: requesting shutdown")
	if a.extractor != nil {
		if err := a.extractor.Shutdown(); err != nil {
			log.Errorf("[ERR] agent: extractor shutdown failed: %v", err)
		}
	}
	if a.applier != nil {
		if err := a.applier.Shutdown(); err != nil {
			log.Errorf("[ERR] agent: applier shutdown failed: %v", err)
		}
	}

	log.Infof("[INFO] agent: shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
