package common

import (
	"fmt"
	"github.com/actiontech/dtle/g"
)

type ExecContext struct {
	Subject    string
	Tp         string
	MaxPayload int
	StateDir   string
}

func ValidateJobName(name string) error {
	if len(name) > g.JobNameLenLimit {
		return fmt.Errorf("job name too long. jobName %v lenLimit %v", name, g.JobNameLenLimit)
	}
	return nil
}
