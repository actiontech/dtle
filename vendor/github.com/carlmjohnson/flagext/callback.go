package flagext

import "flag"

// Callback is a convenience function for defining a Set(string) error
// function on a flag.FlagSet without constructing a new type.
//
// If nil, fl defaults to flag.CommandLine.
// Value is only used for showing the default in usage help.
//
// Deprecated: Use flag.Func in Go 1.16+.
func Callback(fl *flag.FlagSet, name, value, usage string, cb func(string) error) {
	fl = flagOrDefault(fl)

	fl.Var(callback{cb, value}, name, usage)
}

type callback struct {
	fn  func(string) error
	val string
}

func (cb callback) Set(val string) error {
	return cb.fn(val)
}

// String implements flag.Value
func (cb callback) String() string {
	return cb.val
}
