package flagext

import (
	"flag"
	"fmt"
	"strings"
)

// MustHave is a convenience function that checks that the named flags
// were set on fl. Missing flags are treated with the policy of
// fl.ErrorHandling(): ExitOnError, ContinueOnError, or PanicOnError.
// Returned errors will have type MissingFlagsError.
//
// If nil, fl defaults to flag.CommandLine.
func MustHave(fl *flag.FlagSet, names ...string) error {
	fl = flagOrDefault(fl)
	seen := listVisitedFlagNames(fl)
	var missing MissingFlagsError
	for _, name := range names {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return handleErr(fl, missing)
}

// MissingFlagsError is the error type returned by MustHave.
type MissingFlagsError []string

func (missing MissingFlagsError) Error() string {
	if len(missing) == 0 {
		return "MissingFlagsError<empty>"
	}
	if len(missing) == 1 {
		return fmt.Sprintf("missing required flag: %s", missing[0])
	}
	return fmt.Sprintf("missing %d required flags: %s",
		len(missing), strings.Join(missing, ", "))
}

// MustHaveArgs is a convenience function that checks that fl.NArg()
// is within the bounds min and max (inclusive). Use max -1 to indicate
// no maximum value. MustHaveArgs uses the policy of  fl.ErrorHandling():
// ExitOnError, ContinueOnError, or PanicOnError.
//
// If nil, fl defaults to flag.CommandLine.
func MustHaveArgs(fl *flag.FlagSet, min, max int) error {
	fl = flagOrDefault(fl)
	noMax := max < 0
	if max < min && !noMax {
		panic("mismatched arguments to MustHaveArgs")
	}
	n := fl.NArg()
	var err error
	switch {
	case n >= min && (noMax || n <= max):
		return nil
	case min == max && min != 1:
		err = fmt.Errorf("must have %d args; got %d", min, n)
	case min == max:
		err = fmt.Errorf("must have 1 arg; got %d", n)
	case n < min && noMax:
		err = fmt.Errorf("must have at least %d args; got %d", min, n)
	default:
		err = fmt.Errorf("must have between %d and %d args; got %d", min, max, n)
	}
	return handleErr(fl, err)
}
