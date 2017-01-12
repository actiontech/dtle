package agent

import (
	"fmt"
	"sync"

	"github.com/ngaut/log"

	uconf "udup/config"
	usql "udup/agent/mysql"
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
	if err := a.applier.initiateApplier(); err != nil {
		return fmt.Errorf("applier run failed: %v", err)
	}

	for i := 0; i < a.config.WorkerCount; i++ {
		go func(){
			if err := a.applier.ApplyEventQuery(a.applier.dbs[i], a.applier.eventChans[i]); err != nil {
				a.config.PanicAbort <- err
			}
		}()
	}
	return nil
}

// setupExtract is used to setup the extractor if enabled
func (a *Agent) setupExtract() error {
	if !a.config.Extract.Enabled {
		return nil
	}

	// Create the extractor
	a.extractor = NewExtractor(a.config)
	if err := a.extractor.initiateExtractor(); err != nil {
		return fmt.Errorf("extractor run failed: %v", err)
	}

	a.extractor.AddListener(
		false,
		func(ev *usql.StreamEvent) error {
			if err := a.extractor.natsConn.Publish("subject", ev); err != nil {
				return err
			}
			return nil
		},
	)

	go func() {
		log.Debugf("Beginning streaming")
		err := a.extractor.streamEvents()
		if err != nil {
			a.config.PanicAbort <- err
		}
		log.Debugf("Done streaming")
	}()
	return nil
}

// listenOnPanicAbort aborts on abort request
func (a *Agent) listenOnPanicAbort() {
	err := <-a.config.PanicAbort
	log.Errorf("agent run failed: %v", err)
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
