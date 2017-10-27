package sql

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

	umconf "udup/internal/config/mysql"
)

var (
	sanitizeQuotesRegexp = regexp.MustCompile("('[^']*')")
	renameColumnRegexp   = regexp.MustCompile(`(?i)\bchange\s+(column\s+|)([\S]+)\s+([\S]+)\s+`)
	dropColumnRegexp     = regexp.MustCompile(`(?i)\bdrop\s+(column\s+|)([\S]+)$`)
)

type Parser struct {
	ColumnRenameMap map[string]string
	DroppedColumns  map[string]bool
}

func NewParser() *Parser {
	return &Parser{
		ColumnRenameMap: make(map[string]string),
		DroppedColumns:  make(map[string]bool),
	}
}

func (this *Parser) tokenizeAlterStatement(alterStatement string) (tokens []string, err error) {
	terminatingQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == terminatingQuote:
			terminatingQuote = rune(0)
			return false
		case terminatingQuote != rune(0):
			return false
		case c == '\'':
			terminatingQuote = c
			return false
		case c == '(':
			terminatingQuote = ')'
			return false
		default:
			return c == ','
		}
	}

	tokens = strings.FieldsFunc(alterStatement, f)
	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}
	return tokens, nil
}

func (this *Parser) sanitizeQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = sanitizeQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

func (this *Parser) parseAlterToken(alterToken string) (err error) {
	{
		// rename
		allStringSubmatch := renameColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			if unquoted, err := strconv.Unquote(submatch[3]); err == nil {
				submatch[3] = unquoted
			}
			this.ColumnRenameMap[submatch[2]] = submatch[3]
		}
	}
	{
		// drop
		allStringSubmatch := dropColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			this.DroppedColumns[submatch[2]] = true
		}
	}
	return nil
}

func (this *Parser) ParseAlterStatement(alterStatement string) (err error) {
	alterTokens, _ := this.tokenizeAlterStatement(alterStatement)
	for _, alterToken := range alterTokens {
		alterToken = this.sanitizeQuotesFromAlterStatement(alterToken)
		this.parseAlterToken(alterToken)
	}
	return nil
}

func (this *Parser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range this.ColumnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

func (this *Parser) HasNonTrivialRenames() bool {
	return len(this.GetNonTrivialRenames()) > 0
}

func (this *Parser) DroppedColumnsMap() map[string]bool {
	return this.DroppedColumns
}

type Table struct {
	Schema string
	Name   string

	Columns      []*umconf.Column
	IndexColumns []*umconf.Column
}

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func findColumn(columns []*umconf.Column, indexColumn string) *umconf.Column {
	for _, column := range columns {
		if column.Name == indexColumn {
			return column
		}
	}

	return nil
}

func FindColumns(columns []*umconf.Column, indexColumns []string) []*umconf.Column {
	result := make([]*umconf.Column, 0, len(indexColumns))

	for _, name := range indexColumns {
		column := findColumn(columns, name)
		if column != nil {
			result = append(result, column)
		}
	}

	return result
}

func IgnoreError(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case ErrDatabaseExists, ErrDatabaseNotExists, ErrDatabaseDropExists,
		ErrTableExists, ErrTableNotExists, ErrTableDropExists,
		ErrColumnExists, ErrColumnNotExists, ErrDupKeyName,
		ErrIndexExists, ErrCantDropFieldOrKey, ErrDupKey,
		ErrDupEntry, ErrKeyNotFound:
		return true
	default:
		return false
	}
}

func IgnoreExistsError(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case ErrDatabaseExists, ErrTableExists:
		return true
	default:
		return false
	}
}
