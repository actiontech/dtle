package main

import "math/rand"

func theTargetFunction() string {
	if rand.Int()%2 == 0 {
		if rand.Int()%3 == 0 {
			return "2*3"
		} else {
			return "2*!3"
		}
	} else {
		if rand.Int()%7 == 0 {
			return "!2*7"
		} else {
			return "!2*!7"
		}
	}
}
