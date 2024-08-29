package sizeof

import "unsafe"

const (
	StringIntMap = int(unsafe.Sizeof(map[string]int{}))
	String       = int(unsafe.Sizeof(""))
	Int          = int(unsafe.Sizeof(int(0)))
	UInt32       = int(unsafe.Sizeof(uint32(0)))
	Float32      = int(unsafe.Sizeof(float32(0)))
)
