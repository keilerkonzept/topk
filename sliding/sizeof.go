package sliding

import (
	"unsafe"
)

const (
	sizeofSketchStruct = int(unsafe.Sizeof(Sketch{}))
	sizeofBucketStruct = int(unsafe.Sizeof(Bucket{}))
)
