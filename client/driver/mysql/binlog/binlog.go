package binlog

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/outbrain/golib/sqlutils"
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

type BinlogType int

const (
	BinaryLog BinlogType = iota
	RelayLog
)

// BinlogCoordinates described binary log coordinates in the form of log file & log position.
type BinlogCoordinates struct {
	LogFile string
	LogPos  int64
	Type    BinlogType
}

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
func ParseBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

// DisplayString returns a user-friendly string representation of these coordinates
func (b *BinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", b.LogFile, b.LogPos)
}

// String returns a user-friendly string representation of these coordinates
func (b BinlogCoordinates) String() string {
	return b.DisplayString()
}

// Equals tests equality of this corrdinate and another one.
func (b *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos && b.Type == other.Type
}

// IsEmpty returns true if the log file is empty, unnamed
func (b *BinlogCoordinates) IsEmpty() bool {
	return b.LogFile == ""
}

// SmallerThan returns true if this coordinate is strictly smaller than the other.
func (b *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	if b.LogFile < other.LogFile {
		return true
	}
	if b.LogFile == other.LogFile && b.LogPos < other.LogPos {
		return true
	}
	return false
}

// SmallerThanOrEquals returns true if this coordinate is the same or equal to the other one.
// We do NOT compare the type so we can not use this.Equals()
func (b *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if b.SmallerThan(other) {
		return true
	}
	return b.LogFile == other.LogFile && b.LogPos == other.LogPos // No Type comparison
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (b *BinlogCoordinates) FileSmallerThan(other *BinlogCoordinates) bool {
	return b.LogFile < other.LogFile
}

// FileNumberDistance returns the numeric distance between this corrdinate's file number and the other's.
// Effectively it means "how many roatets/FLUSHes would make these coordinates's file reach the other's"
func (b *BinlogCoordinates) FileNumberDistance(other *BinlogCoordinates) int {
	thisNumber, _ := b.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - thisNumber
}

// FileNumber returns the numeric value of the file, and the length in characters representing the number in the filename.
// Example: FileNumber() of mysqld.log.000789 is (789, 6)
func (b *BinlogCoordinates) FileNumber() (int, int) {
	tokens := strings.Split(b.LogFile, ".")
	numPart := tokens[len(tokens)-1]
	numLen := len(numPart)
	fileNum, err := strconv.Atoi(numPart)
	if err != nil {
		return 0, 0
	}
	return fileNum, numLen
}

// PreviousFileCoordinatesBy guesses the filename of the previous binlog/relaylog, by given offset (number of files back)
func (b *BinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: b.Type}

	fileNum, numLen := b.FileNumber()
	if fileNum == 0 {
		return result, errors.New("Log file number is zero, cannot detect previous file")
	}
	newNumStr := fmt.Sprintf("%d", (fileNum - offset))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(b.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (b *BinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return b.PreviousFileCoordinatesBy(1)
}

// PreviousFileCoordinates guesses the filename of the previous binlog/relaylog
func (b *BinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: b.Type}

	fileNum, numLen := b.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(b.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

// FileSmallerThan returns true if this coordinate's file is strictly smaller than the other's.
func (b *BinlogCoordinates) DetachedCoordinates() (isDetached bool, detachedLogFile string, detachedLogPos string) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(b.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, "", ""
	}
	return true, detachedCoordinatesSubmatch[1], detachedCoordinatesSubmatch[2]
}

func GetReplicationBinlogCoordinates(db *gosql.DB) (readBinlogCoordinates *BinlogCoordinates, executeBinlogCoordinates *BinlogCoordinates, err error) {
	err = sqlutils.QueryRowsMap(db, `show slave status`, func(m sqlutils.RowMap) error {
		readBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Master_Log_File"),
			LogPos:  m.GetInt64("Read_Master_Log_Pos"),
		}
		executeBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("Relay_Master_Log_File"),
			LogPos:  m.GetInt64("Exec_Master_Log_Pos"),
		}
		return nil
	})
	return readBinlogCoordinates, executeBinlogCoordinates, err
}

func GetSelfBinlogCoordinates(db *gosql.DB) (selfBinlogCoordinates *BinlogCoordinates, err error) {
	err = sqlutils.QueryRowsMap(db, `show master status`, func(m sqlutils.RowMap) error {
		selfBinlogCoordinates = &BinlogCoordinates{
			LogFile: m.GetString("File"),
			LogPos:  m.GetInt64("Position"),
		}
		return nil
	})
	return selfBinlogCoordinates, err
}
