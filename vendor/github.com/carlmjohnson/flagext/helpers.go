package flagext

import (
	"flag"
	"fmt"
	"os"
)

func listVisitedFlagNames(fl *flag.FlagSet) map[string]bool {
	seen := make(map[string]bool)
	fl.Visit(func(f *flag.Flag) {
		seen[f.Name] = true
	})
	return seen
}

func flagOrDefault(fl *flag.FlagSet) *flag.FlagSet {
	if fl == nil {
		return flag.CommandLine
	}
	return fl
}

func handleErr(fl *flag.FlagSet, err error) error {
	fmt.Fprintln(fl.Output(), err)
	if fl.Usage != nil {
		fl.Usage()
	}
	switch fl.ErrorHandling() {
	case flag.PanicOnError:
		panic(err)
	case flag.ExitOnError:
		os.Exit(2)
	}
	return err
}
