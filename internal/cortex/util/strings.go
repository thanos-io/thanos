package util

import "unsafe"

// StringsContain returns true if the search value is within the list of input values.
func StringsContain(values []string, search string) bool {
	for _, v := range values {
		if search == v {
			return true
		}
	}

	return false
}

// StringsMap returns a map where keys are input values.
func StringsMap(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

// StringsClone returns a copy input s
// see: https://github.com/golang/go/blob/master/src/strings/clone.go
func StringsClone(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}
