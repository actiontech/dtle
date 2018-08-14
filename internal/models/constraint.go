package models

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-version"
)

const (
	ConstraintDistinctProperty = "distinct_property"
	ConstraintDistinctHosts    = "distinct_hosts"
	ConstraintRegex            = "regexp"
	ConstraintVersion          = "version"
	ConstraintSetContains      = "set_contains"
)

// Constraints are used to restrict placement options.
type Constraint struct {
	LTarget string // Left-hand target
	RTarget string // Right-hand target
	Operand string // Constraint operand (<=, <, =, !=, >, >=), contains, near
	str     string // Memoized string
}

// Equal checks if two constraints are equal
func (c *Constraint) Equal(o *Constraint) bool {
	return c.LTarget == o.LTarget &&
		c.RTarget == o.RTarget &&
		c.Operand == o.Operand
}

func (c *Constraint) Copy() *Constraint {
	if c == nil {
		return nil
	}
	nc := new(Constraint)
	*nc = *c
	return nc
}

func (c *Constraint) String() string {
	if c.str != "" {
		return c.str
	}
	c.str = fmt.Sprintf("%s %s %s", c.LTarget, c.Operand, c.RTarget)
	return c.str
}

func (c *Constraint) Validate() error {
	var mErr multierror.Error
	if c.Operand == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing constraint operand"))
	}

	// Perform additional validation based on operand
	switch c.Operand {
	case ConstraintRegex:
		if _, err := regexp.Compile(c.RTarget); err != nil {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Regular expression failed to compile: %v", err))
		}
	case ConstraintVersion:
		if _, err := version.NewConstraint(c.RTarget); err != nil {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Version constraint is invalid: %v", err))
		}
	}
	return mErr.ErrorOrNil()
}
