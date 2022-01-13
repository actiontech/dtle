package main

import (
	"github.com/actiontech/dtle/helper/u"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/vm"

	_ "github.com/actiontech/dtle/drivers/mysql/common"
)

type M map[string]interface{}

func main() {
	expect(New1("id is null", M{"id": 2}), false)
	expect(New1("id is null", M{"id": "hello"}), false)
	expect(New1("id is null", M{"id": nil}), true)
	expect(New1("id is not null", M{"id": 2}), true)
	expect(New1("id is not null", M{"id": nil}), false)
	println(New1("pow(val, 4) = 15", M{"val": 2}))
	println(New1("pow(val, 4) = 16", M{"val": 2}))
	println(New1("char_length(val) > 3", M{"val": "h"}))
	println(New1("char_length(val) > 3", M{"val": "hello"}))
	println(New1("lcase(val) = 'ASD'", M{"val": "ASD"}))
	println(New1("unix_timestamp(val) = 1536316405", M{"val": "2018-09-07 10:33:25"}))
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
