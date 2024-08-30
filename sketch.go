package topk

import (
	"math"
	"math/rand/v2"
	"slices"
	"sort"

	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/internal/sizeof"
)

type Bucket struct {
	Fingerprint uint32
	Count       uint32
}

type Sketch struct {
	K     int // Keep track of top `K` items in the min-heap..
	Width int // Number of buckets per hash function.
	Depth int // Number of hash functions.

	// `math.Pow(Decay, i)` is the probability that a flow's counter with value `i` is decremented on collision.
	Decay float32
	// Look-up table for powers of `Decay`. The value at `i` is `math.Pow(Decay, i)`
	DecayLUT []float32

	Buckets []Bucket  // Sketch counters.
	Heap    *heap.Min // Top-K min-heap.
}

func New(k int, opts ...Option) *Sketch {
	log_k := int(math.Ceil(math.Log(float64(k))))

	// default settings
	out := Sketch{
		K:     k,
		Width: intMax(256, k*log_k),
		Depth: intMax(3, log_k),
		Decay: 0.9,
	}

	for _, o := range opts {
		o(&out)
	}

	if len(out.DecayLUT) == 0 {
		// if not specified, default to 256
		out.DecayLUT = make([]float32, 256)
	}

	out.Heap = heap.NewMin(out.K)
	out.initBuckets()
	out.initDecayLUT()

	return &out
}

func (me *Sketch) initDecayLUT() {
	for i := range me.DecayLUT {
		me.DecayLUT[i] = float32(math.Pow(float64(me.Decay), float64(i)))
	}
}

func (me *Sketch) initBuckets() {
	me.Buckets = make([]Bucket, me.Width*me.Depth)
}

// SizeBytes returns the current size of the sketch in bytes.
func (me *Sketch) SizeBytes() int {
	bucketsSize := (sizeofBucketStruct) * len(me.Buckets)
	heapSize := me.Heap.SizeBytes()
	decayTableSize := len(me.DecayLUT) * sizeof.Float32
	return sizeofSketchStruct +
		bucketsSize +
		heapSize +
		decayTableSize
}

// Count returns the estimated count of the given item.
func (me *Sketch) Count(item string) uint32 {
	if i := me.Heap.Find(item); i >= 0 {
		b := me.Heap.Items[i]
		if b.Item == item {
			return b.Count
		}
	}

	fingerprint := Fingerprint(item)
	var max uint32

	for i := range me.Depth {
		b := &me.Buckets[BucketIndex(item, i, me.Width)]
		if b.Fingerprint != fingerprint {
			continue
		}
		max = uint32Max(max, b.Count)
	}

	return max
}

// Incr counts a single instance of the given item.
func (me *Sketch) Incr(item string) bool {
	return me.Add(item, 1)
}

// Add increments the given item's count by the given increment.
// Returns whether the item is in the top K.
func (me *Sketch) Add(item string, increment uint32) bool {
	var max uint32
	fingerprint := Fingerprint(item)

	width := me.Width
	for i := range me.Depth {
		k := BucketIndex(item, i, width)
		b := &me.Buckets[k]
		count := b.Count
		switch {
		// empty bucket (zero count)
		case count == 0:
			b.Fingerprint = fingerprint
			b.Count = increment
			count = increment

		// this flow's bucket (equal fingerprint)
		case b.Fingerprint == fingerprint:
			b.Count = increment
			count += increment

		// another flow's bucket (nonequal fingerprint)
		default:
			// can't be inlined, so not factored out
			var decay float32
			lookupTableSize := uint32(len(me.DecayLUT))
			for incrementRemaining := increment; incrementRemaining > 0; incrementRemaining-- {
				if count < lookupTableSize {
					decay = me.DecayLUT[count]
				} else {
					decay = float32(math.Pow(
						float64(me.DecayLUT[lookupTableSize-1]),
						float64(count/(lookupTableSize-1)))) * me.DecayLUT[count%(lookupTableSize-1)]
				}
				if rand.Float32() < decay {
					count--
					if count == 0 {
						b.Fingerprint = fingerprint
						count = incrementRemaining
						break
					}
				}
			}
		}

		b.Count = count
		max = uint32Max(max, count)
	}

	return me.Heap.Update(item, fingerprint, max)
}

// Query returns whether the given item is in the top K items by count.
func (me *Sketch) Query(item string) bool {
	return me.Heap.Contains(item)
}

// Iter iterates over the top K items.
func (me *Sketch) Iter(yield func(*heap.Item) bool) {
	for i := range me.Heap.Items {
		if me.Heap.Items[i].Count == 0 {
			continue
		}
		if !yield(&me.Heap.Items[i]) {
			break
		}
	}
}

// SortedSlice returns the top K items as a sorted slice.
func (me *Sketch) SortedSlice() []heap.Item {
	out := slices.Clone(me.Heap.Items)

	sort.SliceStable(out, func(i, j int) bool {
		ci, cj := out[i].Count, out[j].Count
		if ci == cj {
			return out[i].Item < out[j].Item
		}
		return ci > cj
	})

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
	clear(me.Buckets)
	clear(me.Heap.Items)
	clear(me.Heap.Index)
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
