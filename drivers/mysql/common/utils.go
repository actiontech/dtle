/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package common

import (
	"regexp"
	"strconv"
)

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
