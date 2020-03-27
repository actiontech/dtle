package main

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(Analyzer)
}

var Analyzer = &analysis.Analyzer{
	Name:     "hclogvet",
	Doc:      "check hclog invocations",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

var checkHCLogFunc = map[string]bool{
	"Trace": true,
	"Debug": true,
	"Info":  true,
	"Warn":  true,
	"Error": true,
}

func run(pass *analysis.Pass) (interface{}, error) {
	for _, f := range pass.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			fun, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}

			typ := pass.TypesInfo.Types[fun]
			sig, ok := typ.Type.(*types.Signature)
			if !ok {
				return true
			} else if sig == nil {
				return true // the call is not on of the form x.f()
			}

			recv := pass.TypesInfo.Types[fun.X]
			if recv.Type == nil {
				return true
			}

			if !isNamedType(recv.Type, "github.com/hashicorp/go-hclog", "Logger") {
				return true
			}

			if _, ok := checkHCLogFunc[fun.Sel.Name]; !ok {
				return true
			}

			// arity should be odd, with the log message being first and then followed by K/V pairs
			if numArgs := len(call.Args); numArgs%2 != 1 {
				pairs := numArgs / 2
				noun := "pairs"
				if pairs == 1 {
					noun = "pair"
				}
				pass.Reportf(call.Lparen, "invalid number of log arguments to %s (%d valid %s only)", fun.Sel.Name, pairs, noun)
			}

			return true
		})
	}

	return nil, nil
}

// isNamedType reports whether t is the named type path.name.
func isNamedType(t types.Type, path, name string) bool {
	n, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := n.Obj()
	return obj.Name() == name && isPackage(obj.Pkg(), path)
}

// isPackage reports whether pkg has path as the canonical path,
// taking into account vendoring effects
func isPackage(pkg *types.Package, path string) bool {
	if pkg == nil {
		return false
	}

	return pkg.Path() == path ||
		strings.HasSuffix(pkg.Path(), "/vendor/"+path)
}
