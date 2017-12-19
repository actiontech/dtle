// Copyright 2012 The Go Authors for the original package path/filepath.
// Copyright 2012 Martin Schnabel for modifications.
// All rights reserved. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package glob

import "testing"

func TestMetaRegexp(t *testing.T) {
	expr, err := Default().metaRegexp()
	if err != nil {
		t.Fatal(err)
	}
	match := []string{
		`fo*`,
		`fo?`,
		`fo[o]`,
		`*oo`,
	}
	fail := []string{
		`foo`,
		`foo/bar`,
		`foo\*`,
		`fo\?`,
		`fo\[o\]`,
	}

	for _, str := range match {
		if !expr.MatchString(str) {
			t.Errorf("%s must match", str)
		}
	}
	for _, str := range fail {
		if expr.MatchString(str) {
			t.Errorf("%s must fail", str)
		}
	}
}

func TestGlobStrings(t *testing.T) {
	list := []string{
		"a.b.c",
		"a.b.c.d",
		"a.bar",
		"a.foo",
		"b.bar",
		"b.foo",
	}
	tests := []struct {
		pat string
		res []string
	}{
		{"a.bar", []string{
			"a.bar",
		}},
		{"*", []string{
			"a",
			"b",
		}},
		{"a.*", []string{
			"a.b", "a.bar", "a.foo",
		}},
		{"a.b*", []string{
			"a.b", "a.bar",
		}},
		{"*.bar", []string{
			"a.bar", "b.bar",
		}},
		{"**", []string{
			"a",
			"a.b",
			"a.b.c",
			"a.b.c.d",
			"a.bar",
			"a.foo",
			"b",
			"b.bar",
			"b.foo",
		}},
		{"**.[^c-f]**", []string{
			"a.b",
			"a.b.c",
			"a.b.c.d",
			"a.bar",
			"b.bar",
		}},
	}
	c := Default()
	c.Separator = '.'
	g, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		res, err := g.GlobStrings(list, test.pat)
		if err != nil {
			t.Error(err)
			continue
		}
		if !equals(res, test.res) {
			t.Errorf("pattern %q got:\n%v ", test.pat, res)
		}
	}
}

func TestFilepaths(t *testing.T) {
	list := []string{
		"a/b/c",
		"d/e/f",
		"g/h/i",
	}
	tests := []struct {
		pat string
		res []string
	}{
		{"", []string{"a", "d", "g"}},
		{"a", []string{"a/b"}},
		{"a/b", []string{"a/b/c"}},
	}
	g := defaultGlobber
	for _, test := range tests {
		res := g.filepaths(list, test.pat, false)
		if !equals(res, test.res) {
			t.Errorf("pattern %q got:\n%v ", test.pat, res)
		}
	}
}

func TestMatch(t *testing.T) {
	g := defaultGlobber
	match := []string{
		`foo/bar/baz`,
		`foo/*/baz`,
		`**/baz`,
		`**baz`,
		`foo/**`,
		`**z`,
		`f**z`,
		`?o**z`,
		`?o**z`,
		`**/*z`,
	}
	fail := []string{
		`foo`,
		`foo/*`,
		`*/baz`,
		`o**z`,
		`**/*r`,
	}
	var str = "foo/bar/baz"
	for _, pat := range match {
		if m, err := g.Match(pat, str); !m {
			t.Errorf("%s must match %v", pat, err)
		}
	}
	for _, pat := range fail {
		if m, err := g.Match(pat, str); m {
			t.Errorf("%s must fail %v", pat, err)
		}
	}
}

func equals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, str := range a {
		if str != b[i] {
			return false
		}
	}
	return true
}
