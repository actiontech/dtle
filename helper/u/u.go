package u

import (
	"fmt"
)

func PanicIfErr(err interface{}) {
	if err != nil {
		panic(err)
	}
}

func BytesToString(bs interface{}) string {
	return string(bs.([]byte))
}

// eliminate "declared and not used" error
func Use(vals ...interface{}) {

}

func Printlnf(format string, a ...interface{}) {
	fmt.Printf(format, a...)
	println()
}

