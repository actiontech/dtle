package agent

import (
	uconf "udup/config"

	"github.com/ngaut/log"
)

type Extractor struct {
	cfg *uconf.Config
}

func NewExtractor(cfg *uconf.Config) *Extractor {
	return &Extractor{
		cfg: cfg,
	}
}

func (e *Extractor) initiateExtractor() error {
	log.Infof("Extract binlog events from the datasource :%v", e.cfg.Extract.ConnCfg)
	return nil
}

func (e *Extractor) Shutdown() error {
	return nil
}
