package flagext

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// ParseEnv lists any unset flags, checks whether a corresponding
// environment variable exists, and if so calls Set with its value.
// Flag names are prefixed and converted to SCREAMING_SNAKE_CASE when
// looking up environment variables.
func ParseEnv(fl *flag.FlagSet, prefix string) error {
	fl = flagOrDefault(fl)
	seen := listVisitedFlagNames(fl)

	var nameAndVals [][2]string
	fl.VisitAll(func(fn *flag.Flag) {
		if !seen[fn.Name] {
			key := kebabToUpperSnake(prefix, fn.Name)
			if val, ok := os.LookupEnv(key); ok {
				nameAndVals = append(nameAndVals, [2]string{fn.Name, val})
			}
		}
	})
	for i := range nameAndVals {
		name := nameAndVals[i][0]
		val := nameAndVals[i][1]
		if err := fl.Set(name, val); err != nil {
			err = fmt.Errorf("invalid value %q for flag -%s: %v", val, name, err)
			return handleErr(fl, err)
		}
	}
	return nil
}

func kebabToUpperSnake(prefix, name string) string {
	s := name
	if prefix != "" {
		s = prefix + "_" + name
	}
	return strings.Map(func(r rune) rune {
		switch {
		case 'a' <= r && r <= 'z':
			return r + 'A' - 'a'
		case 'A' <= r && r <= 'Z',
			'0' <= r && r <= '9':
			return r
		}
		return '_'
	}, s)
}
