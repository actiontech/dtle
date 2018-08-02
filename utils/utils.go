/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package utils

// Return a substring of limited lenth.
func StrLim(s string, lim int) string {
	if lim < len(s) {
		return s[:lim]
	} else {
		return s
	}
}

// Return s1 if it is not empty, or else s2.
func StringElse(s1 string, s2 string) string {
	if s1 != "" {
		return s1
	} else {
		return s2
	}
}
