package parser

import (
	"bytes"
	"fmt"
	"github.com/sjjian/oracle-sql-parser/ast"
	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
	"strconv"
	"strings"
)

var lexer *lexmachine.Lexer

func token(tokenId int) lexmachine.Action {
	return func(s *lexmachine.Scanner, m *machines.Match) (interface{}, error) {
		return s.Token(tokenId, string(m.Bytes), m), nil
	}
}

func skip(*lexmachine.Scanner, *machines.Match) (interface{}, error) {
	return nil, nil
}

func AddTokenBetween(tokenId int, start []byte, end byte) {
	lexer.Add(start, func(scan *lexmachine.Scanner, match *machines.Match) (interface{}, error) {
		var buf bytes.Buffer
		match.EndLine = match.StartLine
		match.EndColumn = match.StartColumn
		for tc := scan.TC; tc < len(scan.Text); tc++ {
			curByte := scan.Text[tc]

			// calculate location
			match.EndColumn += 1
			if curByte == '\n' {
				match.EndLine += 1
			}
			// match end
			if curByte == end {
				scan.TC = tc + 1
				match.TC = scan.TC
				match.Bytes = buf.Bytes()
				return scan.Token(tokenId, buf.String(), match), nil
			} else {
				// between start and end
				buf.WriteByte(curByte)
			}
		}
		return nil, fmt.Errorf("unclosed %s with %s, staring at %d, (%d, %d)",
			string(start), string(end), match.TC, match.StartLine, match.StartColumn)
	})
}

func AddIdentToken(tokenId int, rs string) {
	l := strings.ToLower(rs)
	u := strings.ToUpper(rs)
	var regex bytes.Buffer
	for i := range l {
		if u[i] == l[i] {
			regex.WriteByte(u[i])
		} else {
			regex.WriteString("[")
			regex.WriteByte(u[i])
			regex.WriteByte(l[i])
			regex.WriteString("]")
		}
	}
	lexer.Add(regex.Bytes(), token(tokenId))
}

var stdTokenMap = map[string]int{
	`\*`: int('*'),
	`\(`: int('('),
	`\)`: int(')'),
	`\.`: int('.'),
	`,`:  int(','),
	`;`:  int(';'),
}

var reservedKeyword = map[string]int{
	"add":        _add,
	"all":        _all,
	"alter":      _alter,
	"as":         _as,
	"asc":        _asc,
	"by":         _by,
	"char":       _char,
	"cluster":    _cluster,
	"column":     _column,
	"compress":   _compress,
	"create":     _create,
	"date":       _date,
	"decimal":    _decimal,
	"default":    _default,
	"delete":     _delete,
	"desc":       _desc,
	"drop":       _drop,
	"float":      _float,
	"for":        _for,
	"from":       _from,
	"identified": _identified,
	"immediate":  _immediate,
	"increment":  _increment,
	"index":      _index,
	"initial":    _initial,
	"integer":    _integer,
	"into":       _into,
	"is":         _is,
	"level":      _level,
	"long":       _long,
	"maxextents": _maxextents,
	"modify":     _modify,
	"nocompress": _nocompress,
	"not":        _not,
	"null":       _null,
	"number":     _number,
	"on":         _on,
	"online":     _online,
	"optimal":    _optimal,
	"order":      _order,
	"pctfree":    _pctfree,
	"raw":        _raw,
	"rename":     _rename,
	"row":        _row,
	"rowid":      _rowid,
	"rows":       _rows,
	"select":     _select,
	"set":        _set,
	"smallint":   _smallInt,
	"start":      _start,
	"table":      _table,
	"to":         _to,
	"unique":     _unique,
	"validate":   _validate,
	"varchar":    _varchar,
	"varchar2":   _varchar2,
	"with":       _with,
}

var unReservedKeyword = map[string]int{
	"advanced":                _advanced,
	"always":                  _always,
	"archive":                 _archive,
	"at":                      _at,
	"attributes":              _attributes,
	"auto":                    _auto,
	"basic":                   _basic,
	"bfile":                   _bfile,
	"binary_double":           _binaryDouble,
	"binary_float":            _binaryFloat,
	"bitmap":                  _bitmap,
	"blob":                    _blob,
	"blockchain":              _blockchain,
	"buffer_pool":             _buffer_pool,
	"byte":                    _byte,
	"cache":                   _cache,
	"capacity":                _capacity,
	"cascade":                 _cascade,
	"cell_flash_cache":        _cell_flash_cache,
	"character":               _character,
	"checkpoint":              _checkpoint,
	"clob":                    _clob,
	"collate":                 _collate,
	"columns":                 _columns,
	"commit":                  _commit,
	"constraint":              _constraint,
	"constraints":             _constraints,
	"continue":                _continue,
	"creation":                _creation,
	"critical":                _critical,
	"cycle":                   _cycle,
	"data":                    _data,
	"day":                     _day,
	"dec":                     _dec,
	"decrypt":                 _decrypt,
	"deferrable":              _deferrable,
	"deferred":                _deferred,
	"definition":              _definition,
	"delete_all":              _delete_all,
	"disable":                 _disable,
	"disable_all":             _disable_all,
	"distribute":              _distribute,
	"dml":                     _dml,
	"double":                  _double,
	"duplicate":               _duplicate,
	"duplicated":              _duplicated,
	"e":                       _E,
	"enable":                  _enable,
	"enable_all":              _enable_all,
	"encrypt":                 _encrypt,
	"exceptions":              _exceptions,
	"extended":                _extended,
	"external":                _external,
	"filesystem_like_logging": _filesystem_like_logging,
	"flash_cache":             _flash_cache,
	"force":                   _force,
	"foreign":                 _foreign,
	"freelist":                _freelist,
	"freelists":               _freelists,
	"full":                    _full,
	"g":                       _G,
	"generated":               _generated,
	"global":                  _global,
	"groups":                  _groups,
	"heap":                    _heap,
	"high":                    _high,
	"identity":                _identity,
	"ilm":                     _ilm,
	"immutable":               _immutable,
	"indexing":                _indexing,
	"initially":               _initially,
	"initrans":                _initrans,
	"inmemory":                _inmemory,
	"int":                     _int,
	"interval":                _interval,
	"invalidate":              _invalidate,
	"invalidation":            _invalidation,
	"invisible":               _invisible,
	"k":                       _K,
	"keep":                    _keep,
	"key":                     _key,
	"levels":                  _levels,
	"limit":                   _limit,
	"local":                   _local,
	"locking":                 _locking,
	"logging":                 _logging,
	"low":                     _low,
	"m":                       _M,
	"maxsize":                 _maxsize,
	"maxtrans":                _maxtrans,
	"maxvalue":                _maxvalue,
	"medium":                  _medium,
	"memcompress":             _memcompress,
	"memoptimize":             _memoptimize,
	"metadata":                _metadata,
	"minextents":              _minextents,
	"minvalue":                _minvalue,
	"month":                   _month,
	"multivalue":              _multivalue,
	"national":                _national,
	"nchar":                   _nchar,
	"nclob":                   _nclob,
	"next":                    _next,
	"no":                      _no,
	"nocache":                 _nocache,
	"nocycle":                 _nocycle,
	"nologging":               _nologging,
	"nomaxvalue":              _nomaxvalue,
	"nominvalue":              _nominvalue,
	"none":                    _none,
	"noorder":                 _noorder,
	"noparallel":              _noparallel,
	"norely":                  _norely,
	"nosort":                  _nosort,
	"novalidate":              _novalidate,
	"numeric":                 _numeric,
	"nvarchar2":               _nvarchar2,
	"organization":            _organization,
	"p":                       _P,
	"parallel":                _parallel,
	"parent":                  _parent,
	"partial":                 _partial,
	"partition":               _partition,
	"pctincrease":             _pctincrease,
	"pctused":                 _pctused,
	"peverse":                 _peverse,
	"policy":                  _policy,
	"precision":               _precision,
	"preserve":                _preserve,
	"primary":                 _primary,
	"priority":                _priority,
	"private":                 _private,
	"purge":                   _purge,
	"query":                   _query,
	"range":                   _range,
	"read":                    _read,
	"real":                    _real,
	"recycle":                 _recycle,
	"references":              _references,
	"reject":                  _reject,
	"rely":                    _rely,
	"salt":                    _salt,
	"scope":                   _scope,
	"second":                  _second,
	"segment":                 _segment,
	"service":                 _service,
	"sharded":                 _sharded,
	"sharding":                _sharding,
	"sort":                    _sort,
	"spatial":                 _spatial,
	"storage":                 _storage,
	"store":                   _store,
	"subpartition":            _subpartition,
	"substitutable":           _substitutable,
	"t":                       _T,
	"tablespace":              _tablespace,
	"temporary":               _temporary,
	"time":                    _time,
	"timestamp":               _timestamp,
	"unlimited":               _unlimited,
	"unusable":                _unusable,
	"unused":                  _unused,
	"urowid":                  _urowid,
	"usable":                  _usable,
	"using":                   _using,
	"value":                   _value,
	"varying":                 _varying,
	"visible":                 _visible,
	"write":                   _write,
	"xmltype":                 _XMLType,
	"year":                    _year,
	"zone":                    _zone,
}

func init() {
	lexer = lexmachine.NewLexer()

	for keyword, tokenId := range stdTokenMap {
		AddIdentToken(tokenId, keyword)
	}
	for keyword, tokenId := range reservedKeyword {
		AddIdentToken(tokenId, keyword)
	}

	for keyword, tokenId := range unReservedKeyword {
		AddIdentToken(tokenId, keyword)
	}

	lexer.Add([]byte("( |\t|\n|\r)+"), skip)

	lexer.Add([]byte("[a-zA-Z]+\\w*"), token(_nonquotedIdentifier))

	lexer.Add([]byte(`[0-9]+`), func(s *lexmachine.Scanner, m *machines.Match) (interface{}, error) {
		v, err := strconv.Atoi(string(m.Bytes))
		if err != nil {
			return nil, err
		}
		return s.Token(_intNumber, v, m), nil
	})

	AddTokenBetween(_doubleQuoteStr, []byte(`"`), byte('"'))
	AddTokenBetween(_singleQuoteStr, []byte(`'`), byte('\''))
	err := lexer.CompileNFA()
	if err != nil {
		panic(err)
	}
}

type yyLexImpl struct {
	scanner *lexmachine.Scanner
	err     error
	result  []ast.Node
	token   *lexmachine.Token
	lastPos int
}

func NewLexer(s string) (*yyLexImpl, error) {
	scanner, err := lexer.Scanner([]byte(s))
	if err != nil {
		return nil, err
	}
	return &yyLexImpl{
		scanner: scanner,
	}, nil
}

func (l *yyLexImpl) Lex(lval *yySymType) int {
	tok, err, eof := l.scanner.Next()
	if err != nil {
		fmt.Println(err)
		l.err = err
		return 0
	}
	if eof {
		return 0
	}
	token := tok.(*lexmachine.Token)
	l.token = token
	switch v := token.Value.(type) {
	case string:
		lval.str = v
	case int:
		lval.i = v
	}
	return token.Type
}

func (l *yyLexImpl) Error(s string) {
	l.err = fmt.Errorf("syntax error, %s, at line %d:%d", s, l.token.StartLine, l.token.TC)
}
