package heap

import (
	"unsafe"
)

const (
	sizeofMinStruct = int(unsafe.Sizeof(Min{}))
	sizeofItem      = int(unsafe.Sizeof(Item{}))
)
