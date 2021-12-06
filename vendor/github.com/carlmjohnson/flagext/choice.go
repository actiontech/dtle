package flagext

import (
	"flag"
	"fmt"
	"strings"
)

type choice struct {
	choices   []string
	selection *string
}

// Choice implements flag.Value. Pass directly into flag.Var.
// flag.Var sets selection to the value of a command line flag if it is among the choices.
// If the flag value is not among the choices, it returns an error.
func Choice(selection *string, choices ...string) flag.Value {
	return choice{
		choices:   choices,
		selection: selection,
	}
}

func (c choice) Set(val string) error {
	for _, choice := range c.choices {
		if val == choice {
			*c.selection = choice
			return nil
		}
	}
	return fmt.Errorf("%q not in %s", val, strings.Join(c.choices, ", "))
}

func (c choice) String() string {
	if c.selection != nil {
		return *c.selection
	}
	return ""
}

func (c choice) Get() interface{} {
	return c.String()
}

// ChoiceVar is a convenience wrapper for Choice. If nil, fl defaults to flag.CommandLine.
func ChoiceVar(fl *flag.FlagSet, name, value, usage string, choices ...string) *string {
	fl = flagOrDefault(fl)

	fl.Var(Choice(&value, choices...), name, usage)
	return &value
}
