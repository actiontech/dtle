// Copyright 2012 The Go Authors for the original package path/filepath.
// Copyright 2012 Martin Schnabel for modifications.
// All rights reserved. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package glob provides configurable globbing and matching algorithms.
// It can be used to for glob-style matching on string slices.
package glob

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// ErrBadPattern indicates a globbing pattern was malformed.
var ErrBadPattern = errors.New("syntax error in pattern")

var defaultConfig = Config{'/', '*', '?', '[', ']', '^', true}

// Default returns the Config used by the package-level default Globber.
// The values are:
//     Config{
//             Separator: '/',
//             Star:      '*',
//             Quest:     '?',
//             Range:     '[',
//             RangeEnd:  ']',
//             RangeNeg:  '^',
//             GlobStar:  true,
//     }
func Default() Config {
	return defaultConfig
}

// Config represents Globber options. It can be used to change the
// supported pattern control characters and Separator as well as enable
// GlobStar behaviour.
type Config struct {
	Separator byte // Separator separates path element
	Star      byte // Star wildcard matching zero or more characters
	Quest     byte // Quest wildcard matching one character
	Range     byte // Range start matches ranges one character from a specified range
	RangeEnd  byte // RangeEnd ends a Range. Ranges of characters can be specified with '-' e.g. [0-9]
	RangeNeg  byte // RangeNeg negates the Range e.g. [^a-z]
	GlobStar  bool // GlobStar enables multiple Stars to match across directory boundaries
}

var defaultGlobber = func() *Globber {
	g, err := New(defaultConfig)
	if err != nil {
		panic(err)
	}
	return g
}()

// mataRegexp returns a regexp matcher to detect pattern control characters.
func (c Config) metaRegexp() (*regexp.Regexp, error) {
	meta := regexp.QuoteMeta(string([]byte{c.Star, c.Quest, c.Range}))
	return regexp.Compile(`(^|[^\\])[` + meta + `]`)
}

// GlobStrings returns all expanded paths from list matching pattern or nil if there are no matches.
// This function calls the corresponding method of a Globber with default Config and is equivalent to:
//     glob.New(glob.Default()).GlobStrings(list, pattern)
func GlobStrings(list []string, pattern string) (matches []string, err error) {
	return defaultGlobber.GlobStrings(list, pattern)
}

// Match returns true if name matches the shell file name pattern.
// This function calls the corresponding method of a Globber with default Config and is equivalent to:
//     glob.New(glob.Default()).Match(pattern, name)
func Match(pattern, name string) (bool, error) {
	return defaultGlobber.Match(pattern, name)
}

// Globber provides configurable globbing and matching algorithms.
type Globber struct {
	config  Config
	hasmeta *regexp.Regexp
}

// New returns a new Globber with the given Config c.
func New(c Config) (*Globber, error) {
	hasmeta, err := c.metaRegexp()
	if err != nil {
		return nil, err
	}
	return &Globber{c, hasmeta}, nil
}

// GlobStrings returns all expanded paths from list matching pattern or nil if there are no matches.
// The syntax of pattern is the same as in Match. The control characters of the pattern and
// GlobStar matching behaviour can be configured in Config.
// The given list must be sorted in ascending order. New matches are added in lexicographical order.
func (g *Globber) GlobStrings(list []string, pattern string) (matches []string, err error) {
	if len(list) == 0 {
		return
	}
	// check if pattern is a simple path
	if !g.hasmeta.MatchString(pattern) {
		i := sort.SearchStrings(list, pattern)
		if i < len(list) && list[i] == pattern {
			matches = []string{pattern}
		}
		return
	}
	dir, file := g.split(pattern)
	if dir != "" && dir[len(dir)-1] == g.config.Separator {
		dir = dir[:len(dir)-1]
	}
	if !g.hasmeta.MatchString(dir) {
		return g.globStrings(list, dir, file, nil)
	}
	var m []string
	m, err = g.GlobStrings(list, dir)
	if err != nil {
		return
	}
	for _, d := range m {
		matches, err = g.globStrings(list, d, file, matches)
		if err != nil {
			return
		}
	}
	return
}

// split splits path immediately following the final Separator, separating it into
// a directory and file component. If there is no Separator in path, split returns an empty
// dir and file set to path. The returned values have the property that path = dir+file.
func (g *Globber) split(path string) (dir, file string) {
	i := len(path) - 1
	for i >= 0 && path[i] != g.config.Separator {
		i--
	}
	return path[:i+1], path[i+1:]
}

// globStrings searches for strings matching pattern in list and appends them to matches.
// The given list must be sorted in ascending order. New matches are added in lexicographical order.
func (g *Globber) globStrings(list []string, dir, pattern string, matches []string) ([]string, error) {
	m := matches
	matchtree := g.config.GlobStar && g.hasGlobStar(pattern)
	paths := g.filepaths(list, dir, matchtree)
	dirlen := len(dir)
	if dir != "" {
		dirlen++
	}
	for _, path := range paths {
		matched, err := g.Match(pattern, path[dirlen:])
		if err != nil {
			return m, err
		}
		if matched {
			m = append(m, path)
		}
	}
	return m, nil
}

// hasGlobStar returns true if the pattern contains multiple Star wildcards in the first path segment.
func (g *Globber) hasGlobStar(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case g.config.Separator:
			return false
		case g.config.Star:
			if i+1 < len(pattern) && pattern[i+1] == g.config.Star {
				return true
			}
		case '\\':
			i++
		}
	}
	return false
}

// filepaths returns child paths of dir expanded from the given list.
// If tree is true it returns all ancestor paths of dir.
func (g *Globber) filepaths(list []string, dir string, tree bool) (r []string) {
	var dirsep string
	if dir != "" {
		dirsep = dir + string(g.config.Separator)
		i := sort.SearchStrings(list, dirsep)
		if i >= len(list) {
			return
		}
		list = list[i:]
	}
	found := map[string]struct{}{}
	for _, path := range list {
		if dirsep != "" && !strings.HasPrefix(path, dirsep) {
			break
		}
		for j := len(dirsep); j < len(path); j++ {
			for j < len(path) && path[j] != g.config.Separator {
				j++
			}
			path := path[:j]
			if _, ok := found[path]; !ok {
				found[path] = struct{}{}
				r = append(r, path)
			}
			if !tree {
				break
			}
		}
	}
	return
}

// Match returns true if name matches the shell file name pattern.
// The control characters can be changed with glob.Config
// The pattern syntax is:
//
//      pattern:
//              { term }
//      term:
//              Star         matches any sequence of non-Separator characters
//              Star+        matches across directory Separators if GlobStar is enabled
//              Quest        matches any single non-Separator character
//              Range [ RangeNeg ] { character-range } RangeEnd
//                          character class (must be non-empty)
//              c           matches character c (c != '*', '?', '\\', '[')
//              '\\' c      matches character c
//
//      character-range:
//              c           matches character c (c != '\\', '-', RangeEnd)
//              '\\' c      matches character c
//              lo '-' hi   matches character c for lo <= c <= hi
//
// Match requires pattern to match all of name, not just a substring.
// The only possible returned error is ErrBadPattern, when pattern
// is malformed.
//
// On Windows, escaping is disabled. Instead, '\\' is treated as
// path separator.
//
func (g *Globber) Match(pattern, name string) (bool, error) {
Pattern:
	for len(pattern) > 0 {
		var star int
		var chunk string
		star, chunk, pattern = g.scanChunk(pattern)
		if chunk == "" {
			switch {
			// Multiple trailing Stars match rest of string if GlobStar is enabled.
			case star > 1 && g.config.GlobStar:
				return true, nil
			// Trailing Stars matches rest of string unless it has a Separator otherwise.
			case star > 0:
				return strings.Index(name, string(g.config.Separator)) < 0, nil
			}
		}
		// Look for match at current position.
		t, ok, err := g.matchChunk(chunk, name)
		// if we're the last chunk, make sure we've exhausted the name
		// otherwise we'll give a false result even if we could still match
		// using the star
		if ok && (len(t) == 0 || len(pattern) > 0) {
			name = t
			continue
		}
		if err != nil {
			return false, err
		}
		var i int
		switch {
		case star > 1 && g.config.GlobStar:
			// Look for match skipping i+1 bytes and recurse to try branches
			for i = 0; i < len(name); i++ {
				t, ok, err := g.matchChunk(chunk, name[i+1:])
				if ok {
					// if we're the last chunk, make sure we exhausted the name
					if len(pattern) == 0 && len(t) > 0 {
						continue
					}
					i, name = 0, t
					if len(pattern) > 0 {
						ok, _ = g.Match(pattern, name)
						if ok {
							return true, nil
						}
						continue
					}
					continue Pattern
				}
				if err != nil {
					return false, err
				}
			}
		case star > 0:
			// Look for match skipping i+1 bytes.
			// Cannot skip Separator.
			for i = 0; i < len(name) && name[i] != g.config.Separator; i++ {
				t, ok, err := g.matchChunk(chunk, name[i+1:])
				if ok {
					// if we're the last chunk, make sure we exhausted the name
					if len(pattern) == 0 && len(t) > 0 {
						continue
					}
					name = t
					continue Pattern
				}
				if err != nil {
					return false, err
				}
			}
		}
		return false, nil
	}
	return len(name) == 0, nil
}

// scanChunk gets the next segment of pattern, which is a non-star string
// possibly preceded by stars.
func (g *Globber) scanChunk(pattern string) (star int, chunk, rest string) {
	for len(pattern) > 0 && pattern[0] == g.config.Star {
		pattern = pattern[1:]
		star++
	}
	inrange := false
	var i int
Scan:
	for i = 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			// error check handled in matchChunk: bad pattern.
			if i+1 < len(pattern) {
				i++
			}
		case g.config.Range:
			inrange = true
		case g.config.RangeEnd:
			inrange = false
		case g.config.Star:
			if !inrange {
				break Scan
			}
		}
	}
	return star, pattern[0:i], pattern[i:]
}

// matchChunk checks whether chunk matches the beginning of s.
// If so, it returns the remainder of s (after the match).
// Chunk is all single-character operators: literals, char classes, and Quest.
func (g *Globber) matchChunk(chunk, s string) (rest string, ok bool, err error) {
	for len(chunk) > 0 {
		if len(s) == 0 {
			return
		}
		switch chunk[0] {
		case g.config.Range:
			// character class
			r, n := utf8.DecodeRuneInString(s)
			s = s[n:]
			chunk = chunk[1:]
			// We can't end right after Range, we're expecting at least
			// RangeEnd and possibly a RangeNeg.
			if len(chunk) == 0 {
				err = ErrBadPattern
				return
			}
			// possibly negated
			negated := chunk[0] == g.config.RangeNeg
			if negated {
				chunk = chunk[1:]
			}
			// parse all ranges
			match := false
			nrange := 0
			for {
				if len(chunk) > 0 && chunk[0] == g.config.RangeEnd && nrange > 0 {
					chunk = chunk[1:]
					break
				}
				var lo, hi rune
				if lo, chunk, err = g.getEsc(chunk); err != nil {
					return
				}
				hi = lo
				if chunk[0] == '-' {
					if hi, chunk, err = g.getEsc(chunk[1:]); err != nil {
						return
					}
				}
				if lo <= r && r <= hi {
					match = true
				}
				nrange++
			}
			if match == negated {
				return
			}

		case g.config.Quest:
			if s[0] == g.config.Separator {
				return
			}
			_, n := utf8.DecodeRuneInString(s)
			s = s[n:]
			chunk = chunk[1:]

		case '\\':
			chunk = chunk[1:]
			if len(chunk) == 0 {
				err = ErrBadPattern
				return
			}
			fallthrough

		default:
			if chunk[0] != s[0] {
				return
			}
			s = s[1:]
			chunk = chunk[1:]
		}
	}
	return s, true, nil
}

// getEsc gets a possibly-escaped character from chunk, for a character class.
func (g *Globber) getEsc(chunk string) (r rune, nchunk string, err error) {
	if len(chunk) == 0 || chunk[0] == '-' || chunk[0] == g.config.RangeEnd {
		err = ErrBadPattern
		return
	}
	if chunk[0] == '\\' {
		chunk = chunk[1:]
		if len(chunk) == 0 {
			err = ErrBadPattern
			return
		}
	}
	r, n := utf8.DecodeRuneInString(chunk)
	if r == utf8.RuneError && n == 1 {
		err = ErrBadPattern
	}
	nchunk = chunk[n:]
	if len(nchunk) == 0 {
		err = ErrBadPattern
	}
	return
}
