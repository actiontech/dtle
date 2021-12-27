package parser

import (
	"fmt"
	"github.com/sjjian/oracle-sql-parser/ast"
)

func Parser(query string) ([]ast.Node, error) {
	l, err := NewLexer(query)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	//yyDebug = 4
	yyParse(l)
	if l.err != nil {
		return nil, l.err
	}
	return l.result, nil
}
