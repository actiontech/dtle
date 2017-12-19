package datemath

import (
	"testing"
	"time"
)

type testCase struct {
	Exp string
	Res time.Time
	Ok  bool
}

func (tc testCase) test(t *testing.T) {
	r, e := EvalAnchor(anchor, tc.Exp)
	if tc.Ok {
		// No error
		if e != nil {
			t.Errorf("`%s` was supposed to work but did not: %v", tc.Exp, e)
		} else if !r.Equal(tc.Res) {
			t.Errorf("`%s` != `%s` for expression: `%s`", tc.Res, r, tc.Exp)
		}
	} else {
		// Expect an error
		if e == nil {
			t.Errorf("`%s` was supposed to error but did not", tc.Exp)
		}
	}
}

var anchor = time.Date(2014, 12, 31, 23, 59, 59, 0, time.UTC)

var tests []testCase = []testCase{
	// Ok
	{"+1s", anchor.Add(1 * time.Second), true},
	{"now+1s", anchor.Add(1 * time.Second), true},
	{"-1s", anchor.Add(-1 * time.Second), true},
	{"+5m", anchor.Add(5 * time.Minute), true},
	{"-5m", anchor.Add(-5 * time.Minute), true},
	{"-6w", anchor.AddDate(0, 0, -6*7), true},
	{"now-6w", anchor.AddDate(0, 0, -6*7), true},
	{"+5M", anchor.AddDate(0, 5, 0), true},
	{"-5M", anchor.AddDate(0, -5, 0), true},
	{"-100y", time.Date(1914, 12, 31, 23, 59, 59, 0, time.UTC), true},
	{"+1000y", time.Date(3014, 12, 31, 23, 59, 59, 0, time.UTC), true},

	// Bad
	{"1s", zero, false},
	{"-0.5s", zero, false},
	{"1S", zero, false},
	{"++1s.", zero, false},
	{"+5ms", zero, false},
	{"+5m/m", zero, false}, // Rounding isn't supported
}

func TestEval(t *testing.T) {
	for _, tc := range tests {
		tc.test(t)
	}
}
