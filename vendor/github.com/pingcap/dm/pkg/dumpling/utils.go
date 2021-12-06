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

package dumpling

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// ParseMetaData parses mydumper's output meta file and returns binlog location.
// since v2.0.0, dumpling maybe configured to output master status after connection pool is established,
// we return this location as well.
func ParseMetaData(filename, flavor string) (*binlog.Location, *binlog.Location, error) {
	invalidErr := fmt.Errorf("file %s invalid format", filename)
	fd, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer fd.Close()

	var (
		pos          mysql.Position
		gtidStr      string
		useLocation2 = false
		pos2         mysql.Position
		gtidStr2     string

		locPtr  *binlog.Location
		locPtr2 *binlog.Location
	)

	br := bufio.NewReader(fd)

	parsePosAndGTID := func(pos *mysql.Position, gtid *string) error {
		for {
			line, err2 := br.ReadString('\n')
			if err2 != nil {
				return err2
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				return nil
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			switch key {
			case "Log":
				pos.Name = value
			case "Pos":
				pos64, err3 := strconv.ParseUint(value, 10, 32)
				if err3 != nil {
					return err3
				}
				pos.Pos = uint32(pos64)
			case "GTID":
				// multiple GTID sets may cross multiple lines, continue to read them.
				following, err3 := readFollowingGTIDs(br, flavor)
				if err3 != nil {
					return err3
				}
				*gtid = value + following
				return nil
			}
		}
	}

	for {
		line, err2 := br.ReadString('\n')
		if err2 == io.EOF {
			break
		} else if err2 != nil {
			return nil, nil, err2
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		switch line {
		case "SHOW MASTER STATUS:":
			if err3 := parsePosAndGTID(&pos, &gtidStr); err3 != nil {
				return nil, nil, err3
			}
		case "SHOW SLAVE STATUS:":
			// ref: https://github.com/maxbube/mydumper/blob/master/mydumper.c#L434
			for {
				line, err3 := br.ReadString('\n')
				if err3 != nil {
					return nil, nil, err3
				}
				line = strings.TrimSpace(line)
				if len(line) == 0 {
					break
				}
			}
		case "SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */":
			useLocation2 = true
			if err3 := parsePosAndGTID(&pos2, &gtidStr2); err3 != nil {
				return nil, nil, err3
			}
		default:
			// do nothing for Started dump, Finished dump...
		}
	}

	if len(pos.Name) == 0 || pos.Pos == uint32(0) {
		return nil, nil, terror.ErrMetadataNoBinlogLoc.Generate(filename)
	}

	gset, err := gtid.ParserGTID(flavor, gtidStr)
	if err != nil {
		return nil, nil, invalidErr
	}
	loc := binlog.InitLocation(pos, gset)
	locPtr = &loc

	if useLocation2 {
		if len(pos2.Name) == 0 || pos2.Pos == uint32(0) {
			return nil, nil, invalidErr
		}
		gset2, err := gtid.ParserGTID(flavor, gtidStr2)
		if err != nil {
			return nil, nil, invalidErr
		}
		loc2 := binlog.InitLocation(pos2, gset2)
		locPtr2 = &loc2
	}

	return locPtr, locPtr2, nil
}

func readFollowingGTIDs(br *bufio.Reader, flavor string) (string, error) {
	var following strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			return following.String(), nil // return the previous, not including the last line.
		} else if err != nil {
			return "", err
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			return following.String(), nil // end with empty line.
		}

		end := len(line)
		if strings.HasSuffix(line, ",") {
			end = len(line) - 1
		}

		// try parse to verify it
		_, err = gtid.ParserGTID(flavor, line[:end])
		if err != nil {
			// nolint:nilerr
			return following.String(), nil // return the previous, not including this non-GTID line.
		}

		following.WriteString(line)
	}
}

// ParseFileSize parses the size in MiB from input.
func ParseFileSize(fileSizeStr string, defaultSize uint64) (uint64, error) {
	var fileSize uint64
	if len(fileSizeStr) == 0 {
		fileSize = defaultSize
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fileSize = fileSizeMB * units.MiB
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		fileSize = uint64(size)
	} else {
		return 0, err
	}
	return fileSize, nil
}
