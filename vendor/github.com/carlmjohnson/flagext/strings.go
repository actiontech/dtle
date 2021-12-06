package flagext

import (
	"flag"
	"strings"
)

// Strings is a slice of strings useful for accepting multiple option values
type Strings []string

// Set implements flag.Value
func (ss *Strings) Set(val string) error {
	*ss = append(*ss, val)
	return nil
}

// String implements flag.Value
func (ss *Strings) String() string {
	if ss == nil {
		return ""
	}
	return strings.Join(*ss, ", ")
}

// Get implements flag.Getter
func (ss *Strings) Get() interface{} {
	if ss == nil {
		return []string(nil)
	}
	return []string(*ss)
}

// StringsVar is a convenience function for adding a slice of strings to a FlagSet.
// If nil, fl defaults to flag.CommandLine.
func StringsVar(fl *flag.FlagSet, ss *[]string, name, usage string) {
	fl = flagOrDefault(fl)
	fl.Var((*Strings)(ss), name, usage)
}
