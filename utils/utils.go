package utils

// Return a substring of limited lenth.
func StrLim(s string, lim int) string {
	if lim < len(s) {
		return s[:lim]
	} else {
		return s
	}
}
