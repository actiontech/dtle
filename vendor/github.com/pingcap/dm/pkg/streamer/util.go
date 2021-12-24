// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package streamer

import (
	"io"
	"strings"

	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// getNextUUID gets (the nextUUID and its suffix) after the current UUID.
func getNextUUID(currUUID string, uuids []string) (string, string, error) {
	for i := len(uuids) - 2; i >= 0; i-- {
		if uuids[i] == currUUID {
			nextUUID := uuids[i+1]
			_, suffixInt, err := utils.ParseSuffixForUUID(nextUUID)
			if err != nil {
				return "", "", terror.Annotatef(err, "UUID %s", nextUUID)
			}
			return nextUUID, utils.SuffixIntToStr(suffixInt), nil
		}
	}
	return "", "", nil
}

// isIgnorableParseError checks whether is a ignorable error for `BinlogParser.ParseFile`.
func isIgnorableParseError(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "err EOF") {
		// NOTE: go-mysql returned err not includes caused err, but as message, ref: parser.go `parseSingleEvent`
		return true
	} else if errors.Cause(err) == io.EOF {
		return true
	}

	return false
}
