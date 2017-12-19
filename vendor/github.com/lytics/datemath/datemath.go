package datemath

// Evaluates date math expressions like: [ operator ] [ number ] [ unit ] such
// as "+5w".
//
// Operator is either + or -. Units supported are y (year), M (month), w
// (week), d (day), h (hour), m (minute), and s (second).

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var zero = time.Time{}

// Eval evaluates a duration relative to now and returns the time or an error.
func Eval(expression string) (time.Time, error) {
	return EvalAnchor(time.Now(), expression)
}

// evalAnchor evaluates a date expression relative to an anchor time.
func EvalAnchor(anchor time.Time, expression string) (time.Time, error) {
	if len(expression) < 3 {
		return zero, fmt.Errorf("Expression too short: %s", expression)
	}

	if strings.HasPrefix(expression, "now") {
		expression = expression[3:]
	}

	numStr, unit := expression[:len(expression)-1], expression[len(expression)-1]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return zero, fmt.Errorf("Invalid duration `%s` in expression: %s", numStr, expression)
	}

	switch unit {
	case 'y':
		return anchor.AddDate(num, 0, 0), nil
	case 'M':
		return anchor.AddDate(0, num, 0), nil
	case 'w':
		return anchor.AddDate(0, 0, num*7), nil
	case 'd':
		return anchor.AddDate(0, 0, num), nil
	case 'h':
		return anchor.Add(time.Duration(num) * time.Hour), nil
	case 'm':
		return anchor.Add(time.Duration(num) * time.Minute), nil
	case 's':
		return anchor.Add(time.Duration(num) * time.Second), nil
	default:
		return zero, fmt.Errorf("Invalid unit `%s` in expression: %s", string(unit), expression)
	}
}
