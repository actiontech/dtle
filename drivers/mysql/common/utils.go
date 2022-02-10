/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package common

import (
	"fmt"
	"github.com/pingcap/tidb/parser/format"
	"regexp"
	"strconv"
)

const ParserRestoreFlag = format.DefaultRestoreFlags | format.RestoreStringWithoutDefaultCharset

func MysqlVersionInDigit(v string) (int, error) {
	maybeErr := fmt.Errorf("bad format of MySQL version %v", v)
	re := regexp.MustCompile(`^((\d)\.(\d\d?)\.(\d\d?)).*`)
	ss := re.FindStringSubmatch(v)
	if len(ss) != 5 {
		return 0, maybeErr
	}
	m0, err := strconv.Atoi(ss[2])
	if err != nil {
		return 0, maybeErr
	}
	m1, err := strconv.Atoi(ss[3])
	if err != nil {
		return 0, maybeErr
	}
	m2, err := strconv.Atoi(ss[4])
	if err != nil {
		return 0, maybeErr
	}

	return m0*10000 + m1*100 + m2, nil
}
