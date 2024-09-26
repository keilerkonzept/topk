// Package heap implements a min-heap that keeps track of the top-K items in a sketch.
package heap

import (
	"container/heap"

	"github.com/keilerkonzept/topk/internal/sizeof"
)

// Item is an entry in the Min-heap with a fingerprint, the item string, and its count.
type Item struct {
	Fingerprint uint32
	Item        string
	Count       uint32
}

// Min is a min-heap that keeps track of the top-K items.
// It holds a slice of Items, an index map for O(1) lookup, and the total number of stored bytes for the keys.
type Min struct {
	K               int
	Items           []Item
	Index           map[string]int
	StoredKeysBytes int
}

// NewMin creates and returns a new Min-heap with a capacity of up to k items.
func NewMin(k int) *Min {
	return &Min{
		K:     k,
		Items: make([]Item, 0, k),
		Index: make(map[string]int, k),
	}
}

// Ensure Min implements the heap.Interface.
var _ heap.Interface = &Min{}

// SizeBytes calculates the total memory usage of the Min heap in bytes.
// This includes the size of the struct, the Items slice, and the index map.
func (me Min) SizeBytes() int {
	structSize := sizeofMinStruct
	bucketsSize := cap(me.Items)*sizeofItem + me.StoredKeysBytes
	indexSize := sizeof.StringIntMap + (sizeof.Int+sizeof.String)*len(me.Index)
	return structSize + bucketsSize + indexSize
}

// Reinit reinitializes the Min heap, removing all items with a zero count.
func (me *Min) Reinit() {
	heap.Init(me)
	for me.Len() > 0 && me.Items[0].Count == 0 {
		item := me.Items[0].Item
		heap.Pop(me)
		delete(me.Index, item)
	}
}

// Full checks if the Min heap is full.
func (me Min) Full() bool { return len(me.Items) == me.K }

// Len returns the number of items currently in the heap. It implements the [heap.Interface].
func (me Min) Len() int { return len(me.Items) }

// Less compares two items in the heap based on their counts (or lexicographically if counts are equal).
// It is used to maintain heap order and implements the [heap.Interface].
func (me Min) Less(i, j int) bool {
	ic := me.Items[i].Count
	jc := me.Items[j].Count
	if ic == jc {
		return me.Items[i].Item < me.Items[j].Item
	}
	return ic < jc
}

// Swap exchanges two items in the heap and updates their indices in the index map.
// It implements the [heap.Interface].
func (me Min) Swap(i, j int) {
	itemi := me.Items[i].Item
	itemj := me.Items[j].Item
	me.Items[i], me.Items[j] = me.Items[j], me.Items[i]
	me.Index[itemi] = j
	me.Index[itemj] = i
}

// Push adds a new item to the heap. It implements the [heap.Interface].
func (me *Min) Push(x interface{}) {
	b := x.(Item)
	me.Items = append(me.Items, b)
	me.Index[b.Item] = len(me.Items) - 1
}

// Pop removes and returns the minimum item from the heap. It implements the [heap.Interface].
func (me *Min) Pop() interface{} {
	old := me.Items
	n := len(old)
	x := old[n-1]
	me.Items = old[0 : n-1]
	delete(me.Index, x.Item)
	return x
}

// Min returns the minimum count in the heap or 0 if the heap is empty.
func (me Min) Min() uint32 {
	if len(me.Items) == 0 {
		return 0
	}
	return me.Items[0].Count
}

// Find searches for an item by its string value and returns its index in the heap.
// If the item is not found, it returns -1.
func (me Min) Find(item string) (i int) {
	if i, ok := me.Index[item]; ok {
		return i
	}
	return -1
}

// Contains checks if a given item exists in the heap.
func (me Min) Contains(item string) bool {
	_, ok := me.Index[item]
	return ok
}

// Get returns a pointer to the Item corresponding to the given item string.
// If the item is not found, it returns nil.
func (me Min) Get(item string) *Item {
	if i, ok := me.Index[item]; ok {
		return &me.Items[i]
	}
	return nil
}

// Update inserts or updates an item in the heap.
// If the count is smaller than the current minimum count and the heap is full, the update is ignored.
// Otherwise, the item is added or updated in the heap.
func (me *Min) Update(item string, fingerprint uint32, count uint32) bool {
	if count < me.Min() && me.Full() { // not in top k: ignore
		return false
	}

	if i := me.Find(item); i >= 0 { // already in heap: update count
		me.Items[i].Count = count
		heap.Fix(me, i)
		return true
	}

	me.StoredKeysBytes += len(item)

	if !me.Full() { // heap not full: add to heap
		heap.Push(me, Item{
			Count:       count,
			Fingerprint: fingerprint,
			Item:        item,
		})
		return true
	}

	// replace min on heap
	minItem := me.Items[0].Item
	me.StoredKeysBytes -= len(minItem)
	delete(me.Index, minItem)
	me.Items[0] = Item{
		Count:       count,
		Fingerprint: fingerprint,
		Item:        item,
	}
	me.Index[item] = 0
	heap.Fix(me, 0)
	return true
}

// Reset resets the heap.
func (me *Min) Reset() {
	clear(me.Items)
	clear(me.Index)
	me.StoredKeysBytes = 0
	me.Items = me.Items[:0]
}
