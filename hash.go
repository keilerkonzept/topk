package topk

import "github.com/OneOfOne/xxhash"

const hashSeed = 4848280

// Fingerprint returns an item's fingerprint.
func Fingerprint(item string) uint32 {
	return xxhash.ChecksumString32S(item, hashSeed)
}

// BucketIndex returns the counter bucket index for an item in the given row of the sketch.
func BucketIndex(item string, row, width int) int {
	column := int(xxhash.ChecksumString32S(item, uint32(row))) % width
	return row*width + column
}
