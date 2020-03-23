/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/actiontech/kafkas, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package util

import (
	"fmt"
	"strconv"
	"testing"
)

func TestID(t *testing.T) {
	id, err := NewIdWorker(2, 3, SnsEpoch)
	if err != nil {
		t.FailNow()
	}
	sid, err := id.NextId()
	if err != nil {
		t.FailNow()
	}

	bid := []byte(strconv.FormatUint(uint64(sid), 10))
	uid, err := strconv.ParseUint(string(bid), 10, 32)
	if err != nil {
		t.FailNow()
	}
	fmt.Printf("snowflake id: %d", uint32(uid))
}
