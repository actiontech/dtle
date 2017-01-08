package agent

import (
	uconf "udup/config"

	"github.com/ngaut/log"
)

type Applier struct {
	cfg *uconf.Config
}

func NewApplier(cfg *uconf.Config) *Applier {
	return &Applier{
		cfg: cfg,
	}
}

func (a *Applier) initiateApplier() error {
	log.Infof("Apply binlog events onto the datasource :%v", a.cfg.Apply.ConnCfg)
	return nil
}

func (a *Applier) Shutdown() error {
	return nil
}
