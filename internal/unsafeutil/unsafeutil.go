package unsafeutil

import "unsafe"

// Bytes converts a string into a byte slice.
func Bytes(s string) (b []byte) {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
