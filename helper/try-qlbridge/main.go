package main

import (
	"github.com/actiontech/dtle/helper/u"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"
)

type M map[string]interface{}

func main() {
	expect(New1("id is null", M{"id": 2}), false)
	expect(New1("id is null", M{"id": "hello"}), false)
	expect(New1("id is null", M{"id": nil}), true)
	expect(New1("id is not null", M{"id": 2}), true)
	expect(New1("id is not null", M{"id": nil}), false)
}

func New1(where string, data map[string]interface{}) bool {
	ast, err := expr.ParseExpression(where)
	if err != nil {
		u.PanicIfErr(err)
	}

	val, ok := vm.Eval(datasource.NewContextSimpleNative(data), ast)
	if !ok {
		panic("cannot eval")
	}

	r, ok := val.Value().(bool)
	if !ok {
		panic("not bool")
	}

	return r
}

func expect(r1 bool, r2 bool) {
	if r1 != r2 {
		panic("unexpected")
	}
}
