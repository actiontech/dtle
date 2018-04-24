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
