/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package g

import (
	"time"
)

// boolToPtr returns the pointer to a boolean
func BoolToPtr(b bool) *bool {
	return &b
}

// IntToPtr returns the pointer to an int
func IntToPtr(i int) *int {
	return &i
}

// UintToPtr returns the pointer to an uint
func Uint64ToPtr(u uint64) *uint64 {
	return &u
}

// StringToPtr returns the pointer to a string
func StringToPtr(str string) *string {
	return &str
}

// TimeToPtr returns the pointer to a time stamp
func TimeToPtr(t time.Duration) *time.Duration {
	return &t
}

// Helpers for copying generic structures.
func CopyMapStringString(m map[string]string) map[string]string {
	l := len(m)
	if l == 0 {
		return nil
	}

	c := make(map[string]string, l)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func CopyMapStringInt(m map[string]int) map[string]int {
	l := len(m)
	if l == 0 {
		return nil
	}

	c := make(map[string]int, l)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func CopyMapStringFloat64(m map[string]float64) map[string]float64 {
	l := len(m)
	if l == 0 {
		return nil
	}

	c := make(map[string]float64, l)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func CopySliceString(s []string) []string {
	l := len(s)
	if l == 0 {
		return nil
	}

	c := make([]string, l)
	for i, v := range s {
		c[i] = v
	}
	return c
}
