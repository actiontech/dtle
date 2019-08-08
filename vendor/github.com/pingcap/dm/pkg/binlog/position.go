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

package binlog

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/utils"
)

const (
	// in order to differ binlog pos from multi (switched) masters, we added a UUID-suffix field into binlogPos.Name
	// and we also need support: with UUIDSuffix's pos should always > without UUIDSuffix's pos, so we can update from @without to @with automatically
	// conversion: originalPos.NamePrefix + posUUIDSuffixSeparator + UUIDSuffix + binlogFilenameSep + originalPos.NameSuffix => convertedPos.Name
	// UUIDSuffix is the suffix of sub relay directory name, and when new sub directory created, UUIDSuffix is incremented
	// eg. mysql-bin.000003 in c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002 => mysql-bin|000002.000003
	// where `000002` in `c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002` is the UUIDSuffix
	posUUIDSuffixSeparator = "|"
)

// PositionFromStr constructs a mysql.Position from a string representation like `mysql-bin.000001:2345`
func PositionFromStr(s string) (gmysql.Position, error) {
	parsed := strings.Split(s, ":")
	if len(parsed) != 2 {
		return gmysql.Position{}, errors.New("the format should be filename:pos")
	}
	pos, err := strconv.ParseUint(parsed[1], 10, 32)
	if err != nil {
		return gmysql.Position{}, errors.New("the pos should be digital")
	}

	return gmysql.Position{
		Name: parsed[0],
		Pos:  uint32(pos),
	}, nil
}

// RealMySQLPos parses a relay position and returns a mysql position and whether error occurs
// if parsed successfully and `UUIDSuffix` exists, sets position Name to
// `originalPos.NamePrefix + binlogFilenameSep + originalPos.NameSuffix`.
// if parsed failed returns the given position and the traced error.
func RealMySQLPos(pos gmysql.Position) (gmysql.Position, error) {
	parsed, err := ParseFilename(pos.Name)
	if err != nil {
		return pos, errors.Trace(err)
	}

	sepIdx := strings.LastIndex(parsed.BaseName, posUUIDSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posUUIDSuffixSeparator) < len(parsed.BaseName) {
		if !verifyUUIDSuffix(parsed.BaseName[sepIdx+len(posUUIDSuffixSeparator):]) {
			// NOTE: still can't handle the case where `log-bin` has the format of `mysql-bin|666888`.
			return pos, nil // pos is just the real pos
		}
		return gmysql.Position{
			Name: ConstructFilename(parsed.BaseName[:sepIdx], parsed.Seq),
			Pos:  pos.Pos,
		}, nil
	}

	return pos, nil
}

// ExtractPos extracts (uuidWithSuffix, uuidSuffix, originalPos) from input pos (originalPos or convertedPos)
func ExtractPos(pos gmysql.Position, uuids []string) (uuidWithSuffix string, uuidSuffix string, realPos gmysql.Position, err error) {
	if len(uuids) == 0 {
		err = errors.NotValidf("empty UUIDs")
		return
	}

	parsed, err2 := ParseFilename(pos.Name)
	if err2 != nil {
		err = errors.Trace(err)
		return
	}
	sepIdx := strings.LastIndex(parsed.BaseName, posUUIDSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posUUIDSuffixSeparator) < len(parsed.BaseName) {
		realBaseName, masterUUIDSuffix := parsed.BaseName[:sepIdx], parsed.BaseName[sepIdx+len(posUUIDSuffixSeparator):]
		if !verifyUUIDSuffix(masterUUIDSuffix) {
			err = errors.Errorf("invalid UUID suffix %s", masterUUIDSuffix)
			return
		}

		// NOTE: still can't handle the case where `log-bin` has the format of `mysql-bin|666888` and UUID suffix `666888` exists.
		uuid := utils.GetUUIDBySuffix(uuids, masterUUIDSuffix)

		if len(uuid) > 0 {
			// valid UUID found
			uuidWithSuffix = uuid
			uuidSuffix = masterUUIDSuffix
			realPos = gmysql.Position{
				Name: ConstructFilename(realBaseName, parsed.Seq),
				Pos:  pos.Pos,
			}
		} else {
			err = errors.NotFoundf("UUID suffix %s with UUIDs %v", masterUUIDSuffix, uuids)
		}
		return
	}

	// use the latest
	var suffixInt int
	uuid := uuids[len(uuids)-1]
	_, suffixInt, err = utils.ParseSuffixForUUID(uuid)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	uuidWithSuffix = uuid
	uuidSuffix = utils.SuffixIntToStr(suffixInt)
	realPos = pos // pos is realPos
	return
}

// verifyUUIDSuffix verifies suffix whether is a valid UUID suffix.
func verifyUUIDSuffix(suffix string) bool {
	v, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || v <= 0 {
		return false
	}
	return true
}
