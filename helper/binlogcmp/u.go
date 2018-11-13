package main

import (
	"fmt"
)

func panicIfErr(err interface{}) {
	if err != nil {
		panic(err)
	}
}

func bytesToString(bs interface{}) string {
	return string(bs.([]byte))
}

// eliminate "declared and not used" error
func Use(vals ...interface{}) {

}

func printlnf(format string, a ...interface{}) {
	fmt.Printf(format, a...)
	println()
}
