package topk

import "github.com/OneOfOne/xxhash"

const hashSeed = 4848280

func Fingerprint(item string) uint32 {
	return xxhash.ChecksumString32S(item, hashSeed)
}

func BucketIndex(item string, row, width int) int {
	column := int(xxhash.ChecksumString32S(item, uint32(row))) % width
	return row*width + column
}
