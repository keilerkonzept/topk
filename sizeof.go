package topk

import "unsafe"

const (
	sizeofString  = int(unsafe.Sizeof(""))
	sizeofInt     = int(unsafe.Sizeof(int(0)))
	sizeofUInt32  = int(unsafe.Sizeof(uint32(0)))
	sizeofFloat32 = int(unsafe.Sizeof(float32(0)))
)
