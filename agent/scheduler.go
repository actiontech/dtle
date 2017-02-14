package agent

import (
	"expvar"
	"github.com/ngaut/log"
)

var (
	schedulerStarted = expvar.NewString("scheduler_started")
)

type Scheduler struct {
	Started bool
}

func NewScheduler() *Scheduler {
	schedulerStarted.Set("false")
	return &Scheduler{Started: false}
}

func (s *Scheduler) Start(jobs []*Job) {
	for _, job := range jobs {
		if job.Disabled || job.ParentJob != "" {
			continue
		}

		log.Infof("job:%v,scheduler: run job", job.Name)
		go job.Run()
	}
	s.Started = true

	schedulerStarted.Set("true")
}

func (s *Scheduler) Stop() {
	if s.Started {
		log.Infof("scheduler: Stopping scheduler")
		s.Started = false

		schedulerStarted.Set("false")
	}
}

func (s *Scheduler) Restart(jobs []*Job) {
	s.Stop()
	s.Start(jobs)
}
