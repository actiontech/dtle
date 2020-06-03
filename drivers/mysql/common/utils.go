/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package common

import (
	"regexp"
	"strconv"
	"time"
)

// Return a substring of limited lenth.
func StrLim(s string, lim int) string {
	if lim < len(s) {
		return s[:lim]
	} else {
		return s
	}
}

// Return s1 if it is not empty, or else s2.
func StringElse(s1 string, s2 string) string {
	if s1 != "" {
		return s1
	} else {
		return s2
	}
}

func CurrentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func MysqlVersionInDigit(v string) int {
	re := regexp.MustCompile(`^((\d)\.(\d\d?)\.(\d\d?)).*`)
	ss := re.FindStringSubmatch(v)
	if len(ss) != 5 {
		return 0
	}
	m0, err := strconv.Atoi(ss[2])
	if err != nil {
		return 0
	}
	m1, err := strconv.Atoi(ss[3])
	if err != nil {
		return 0
	}
	m2, err := strconv.Atoi(ss[4])
	if err != nil {
		return 0
	}

	return m0*10000 + m1*100 + m2
}
