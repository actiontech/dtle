/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/actiontech/kafkas, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// LoadMap is a mapping of status variable & threshold
// e.g. [Threads_connected: 100, Threads_running: 50]
type LoadMap map[string]int64

func NewLoadMap() LoadMap {
	result := make(map[string]int64)
	return result
}

// NewLoadMap parses a `--*-load` flag (e.g. `--max-load`), which is in multiple
// key-value format, such as:
//   'Threads_running=100,Threads_connected=500'
func ParseLoadMap(loadList string) (LoadMap, error) {
	result := NewLoadMap()
	if loadList == "" {
		return result, nil
	}

	loadConditions := strings.Split(loadList, ",")
	for _, loadCondition := range loadConditions {
		loadTokens := strings.Split(loadCondition, "=")
		if len(loadTokens) != 2 {
			return result, fmt.Errorf("Error parsing load condition: %s", loadCondition)
		}
		if loadTokens[0] == "" {
			return result, fmt.Errorf("Error parsing status variable in load condition: %s", loadCondition)
		}
		if n, err := strconv.ParseInt(loadTokens[1], 10, 0); err != nil {
			return result, fmt.Errorf("Error parsing numeric value in load condition: %s", loadCondition)
		} else {
			result[loadTokens[0]] = n
		}
	}

	return result, nil
}

// Duplicate creates a clone of this map
func (l *LoadMap) Duplicate() LoadMap {
	dup := make(map[string]int64)
	for k, v := range *l {
		dup[k] = v
	}
	return dup
}

// String() returns a string representation of this map
func (l *LoadMap) String() string {
	tokens := []string{}
	for key, val := range *l {
		token := fmt.Sprintf("%s=%d", key, val)
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	return strings.Join(tokens, ",")
}
