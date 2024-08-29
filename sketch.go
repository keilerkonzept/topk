// Package topk implements a sliding HeavyKeeper, as described in "A Sketch Framework for Approximate Data Stream Processing in Sliding Windows" [1]
// [1] https://yangtonghome.github.io/uploads/SlidingSketch_TKDE2022_final.pdf
package topk

import (
	"container/heap"
	"math"
	"math/rand/v2"
	"sort"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/keilerkonzept/sliding-topk/internal/unsafeutil"
)

const (
	hashSeed = 4848280
)

func fingerprint(item []byte) uint32 {
	return xxhash.Checksum32S(item, hashSeed)
}

func bucketIndex(item []byte, row, width int) int {
	column := int(xxhash.Checksum32S(item, uint32(row))) % width
	return row*width + column
}

type Sketch struct {
	K                   int
	Width               int
	Depth               int
	WindowSize          int // N: window size in ticks
	BucketHistoryLength int // d

	Decay            float32
	DecayLookupTable []float32

	NextBucketToExpireIndex int // Index of the next bucket to expire.

	Buckets []Bucket
	Heap    *MinHeap
}

type Option func(*Sketch)

func WithDepth(depth int) Option     { return func(s *Sketch) { s.Depth = depth } }
func WithWidth(width int) Option     { return func(s *Sketch) { s.Width = width } }
func WithDecay(decay float32) Option { return func(s *Sketch) { s.Decay = decay } }
func WithDecayLookupTableSize(n int) Option {
	return func(s *Sketch) { s.DecayLookupTable = make([]float32, n) }
}
func WithBucketHistoryLength(n int) Option {
	return func(s *Sketch) { s.BucketHistoryLength = n }
}

func New(k, windowSize int, opts ...Option) *Sketch {
	log_k := int(math.Ceil(math.Log(float64(k))))

	// default settings
	out := Sketch{
		K:                   k,
		Width:               intMax(256, k*log_k),
		Depth:               intMax(3, log_k),
		WindowSize:          windowSize,
		BucketHistoryLength: windowSize,
		Decay:               0.9,
	}

	for _, opt := range opts {
		opt(&out)
	}

	if len(out.DecayLookupTable) <= 2 {
		out.DecayLookupTable = make([]float32, 256)
	}

	if out.BucketHistoryLength < 1 {
		out.BucketHistoryLength = 1
	}
	if out.BucketHistoryLength >= out.WindowSize {
		out.BucketHistoryLength = out.WindowSize
	}

	out.Heap = NewMinHeap(out.K)
	out.initBuckets()
	out.initLookupTable()

	return &out
}

func (me *Sketch) initLookupTable() {
	for i := range me.DecayLookupTable {
		me.DecayLookupTable[i] = float32(math.Pow(float64(me.Decay), float64(i)))
	}
}

func (me *Sketch) initBuckets() {
	me.Buckets = make([]Bucket, me.Width*me.Depth)
	for i := range me.Buckets {
		me.Buckets[i].Counts = make([]uint32, me.BucketHistoryLength)
	}
}

// SizeBytes returns the current size of the sketch in bytes.
func (me *Sketch) SizeBytes() int {
	bucketsSize := (sizeOfBucketStruct + sizeofUInt32*me.BucketHistoryLength) * len(me.Buckets)
	heapSize := me.Heap.SizeBytes()
	decayTableSize := len(me.DecayLookupTable) * sizeofFloat32
	return bucketsSize + heapSize + decayTableSize
}

// Tick advances time by one unit (of the N units in a window)
func (me *Sketch) Tick() { me.Ticks(1) }

// Ticks advances time by n units (of the N units in a window)
func (me *Sketch) Ticks(n int) {
	if n == 0 {
		return
	}
	tick := me.NextBucketToExpireIndex
	m, d, N := len(me.Buckets), me.BucketHistoryLength, me.WindowSize
	bucketsToAge := (n * d * m) / N
	if bucketsToAge < 1 {
		bucketsToAge = 1
	}
	for i := 0; i < bucketsToAge; i++ {
		me.Buckets[tick].tick()
		tick++
		if tick == m {
			tick = 0
		}
	}
	me.NextBucketToExpireIndex = tick
	me.recountHeapItems()
}

// Count returns the estimated count of the given item.
func (me *Sketch) Count(item string) uint32 {
	if i := me.Heap.Find(item); i >= 0 {
		b := me.Heap.Items[i]
		if b.Item == item {
			return b.Count
		}
	}

	itemBytes := unsafeutil.Bytes(item)
	fingerprint := fingerprint(itemBytes)
	var maxSum uint32

	for i := range me.Depth {
		b := &me.Buckets[bucketIndex(itemBytes, i, me.Width)]
		if b.Fingerprint != fingerprint {
			continue
		}
		maxSum = uint32Max(maxSum, b.CountsSum)
	}

	return maxSum
}

func (me *Sketch) recountHeapItems() {
	// recompute each heap item's count from its buckets,
	// then re-initialize the heap.
	//
	// O(k * depth)
	for i := range me.Heap.Items {
		hb := &me.Heap.Items[i]
		if hb.Count == 0 {
			continue
		}
		fingerprint := hb.Fingerprint
		itemBytes := unsafeutil.Bytes(hb.Item)
		var maxSum uint32

		for i := range me.Depth {
			b := &me.Buckets[bucketIndex(itemBytes, i, me.Width)]
			if b.Fingerprint != fingerprint {
				continue
			}
			maxSum = uint32Max(maxSum, b.CountsSum)
		}
		hb.Count = maxSum
	}

	// O(k)
	heap.Init(me.Heap)
	for me.Heap.Len() > 0 && me.Heap.Items[0].Count == 0 {
		item := me.Heap.Items[0].Item
		heap.Pop(me.Heap)
		delete(me.Heap.Index, item)
		continue
	}
}

// Incr counts a single instance of the given item.
func (me *Sketch) Incr(item string) {
	me.Add(item, 1)
}

// Add increments the given item's count by the given increment.
func (me *Sketch) Add(item string, increment uint32) {
	var maxCount uint32
	itemBytes := unsafeutil.Bytes(item)
	fingerprint := fingerprint(itemBytes)

	for i := range me.Depth {
		k := bucketIndex(itemBytes, i, me.Width)
		b := &me.Buckets[k]
		count := b.CountsSum
		switch {
		// empty bucket (zero count)
		case count == 0:
			b.Fingerprint = fingerprint
			clear(b.Counts)
			b.Counts[0] = increment
			count = increment

		// this flow's bucket (equal fingerprint)
		case b.Fingerprint == fingerprint:
			b.Counts[0] += increment
			count += increment

		// another flow's bucket (nonequal fingerprint)
		default:
			// can't be inlined, so not factored out
			var decay float32
			lookupTableSize := uint32(len(me.DecayLookupTable))
			for incrementRemaining := increment; incrementRemaining > 0; incrementRemaining-- {
				if count < lookupTableSize {
					decay = me.DecayLookupTable[count]
				} else {
					decay = float32(math.Pow(
						float64(me.DecayLookupTable[lookupTableSize-1]),
						float64(count/(lookupTableSize-1)))) * me.DecayLookupTable[count%(lookupTableSize-1)]
				}
				if rand.Float32() < decay {
					countsMinIdx := b.findNonzeroMinimumCount()
					b.Counts[countsMinIdx]--
					count--
					if count == 0 {
						b.Fingerprint = fingerprint
						count = incrementRemaining
						break
					}
				}
			}
		}

		b.CountsSum = count
		maxCount = uint32Max(maxCount, count)
	}

	me.Heap.Update(item, fingerprint, maxCount)
}

// Query returns whether the given item is in the top K items by count.
func (me *Sketch) Query(item string) bool {
	return me.Heap.Contains(item)
}

// TopK returns the top K items as a slice.
func (me *Sketch) TopK() []ItemWithCount {
	out := make([]ItemWithCount, me.K)
	for i, b := range me.Heap.Items {
		if b.Count == 0 {
			continue
		}
		out[i] = ItemWithCount{
			Item:  b.Item,
			Count: b.Count,
		}
	}
	sort.Stable(sort.Reverse(byCount(out)))

	end := len(out)
	for ; end > 0; end-- {
		if out[end-1].Count > 0 {
			break
		}
	}

	return out[:end]
}

// Reset resets the sketch to an empty state.
func (me *Sketch) Reset() {
	me.NextBucketToExpireIndex = 0
	for i := range me.Buckets {
		me.Buckets[i].CountsSum = 0
		me.Buckets[i].Fingerprint = 0
		clear(me.Buckets[i].Counts)
	}
	clear(me.Buckets)
	clear(me.Heap.Items)
	clear(me.Heap.Index)
}

const (
	sizeOfBucketStruct = int(unsafe.Sizeof(Bucket{}))
)

type Bucket struct {
	Fingerprint uint32
	Counts      []uint32
	CountsSum   uint32
}

func (me *Bucket) tick() {
	if me.CountsSum == 0 {
		return
	}

	expired := len(me.Counts) - 1
	me.CountsSum -= me.Counts[expired]

	if me.CountsSum == 0 {
		me.Counts[expired] = 0
		return
	}

	for j := len(me.Counts) - 1; j > 0; j-- {
		me.Counts[j] = me.Counts[j-1]
	}
	me.Counts[0] = 0

}

func (me *Bucket) findNonzeroMinimumCount() int {
	countsMinIdx := 0
	first := true
	var countsMin uint32
	for j, c := range me.Counts {
		if c == 0 {
			continue
		}
		if first || c < countsMin {
			countsMin = c
			countsMinIdx = j
			first = false
		}
	}
	return countsMinIdx
}

type ItemWithCount struct {
	Item  string
	Count uint32
}

type byCount []ItemWithCount

func (a byCount) Len() int      { return len(a) }
func (a byCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byCount) Less(i, j int) bool {
	ic := a[i].Count
	jc := a[j].Count
	if ic == jc {
		return a[i].Item > a[j].Item
	}
	return ic < jc
}

func uint32Max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
