package topk

import (
	"container/heap"
	"unsafe"
)

type HeapItem struct {
	Fingerprint uint32
	Item        string
	Count       uint32
}

type MinHeap struct {
	Items           []HeapItem
	Index           map[string]int
	StoredKeysBytes int
}

func NewMinHeap(k int) *MinHeap {
	return &MinHeap{
		Items: make([]HeapItem, k),
		Index: make(map[string]int, k),
	}
}

var _ heap.Interface = &MinHeap{}

const (
	sizeOfBucketMinHeapStruct = int(unsafe.Sizeof(MinHeap{}))
	sizeOfHeapBucket          = int(unsafe.Sizeof(HeapItem{}))
	sizeOfIndex               = int(unsafe.Sizeof(map[string]int{}))
)

func (h MinHeap) SizeBytes() int {
	structSize := sizeOfBucketMinHeapStruct
	bucketsSize := len(h.Items)*sizeOfHeapBucket + h.StoredKeysBytes
	indexSize := sizeOfIndex + (sizeofInt+sizeofString)*len(h.Index)
	return structSize + bucketsSize + indexSize
}

func (h MinHeap) Full() bool { return len(h.Items) == cap(h.Items) }
func (h MinHeap) Len() int   { return len(h.Items) }
func (h MinHeap) Less(i, j int) bool {
	ic := h.Items[i].Count
	jc := h.Items[j].Count
	if ic == jc {
		return h.Items[i].Item < h.Items[j].Item
	}
	return ic < jc
}
func (h MinHeap) Swap(i, j int) {
	itemi := h.Items[i].Item
	itemj := h.Items[j].Item
	h.Items[i], h.Items[j] = h.Items[j], h.Items[i]
	h.Index[itemi] = j
	h.Index[itemj] = i
}

func (h *MinHeap) Push(x interface{}) {
	b := x.(HeapItem)
	h.Items = append(h.Items, b)
	h.Index[b.Item] = len(h.Items) - 1
}

func (h *MinHeap) Pop() interface{} {
	old := h.Items
	n := len(old)
	x := old[n-1]
	h.Items = old[0 : n-1]
	delete(h.Index, x.Item)
	return x
}

// Min returns the minimum count in the heap or 0 if the heap is empty.
func (h MinHeap) Min() uint32 {
	if len(h.Items) == 0 {
		return 0
	}
	return h.Items[0].Count
}

func (h MinHeap) Find(item string) (i int) {
	if i, ok := h.Index[item]; ok {
		return i
	}
	return -1
}

func (h MinHeap) Contains(item string) bool {
	_, ok := h.Index[item]
	return ok
}

func (h MinHeap) Get(item string) *HeapItem {
	if i, ok := h.Index[item]; ok {
		return &h.Items[i]
	}
	return nil
}

func (h *MinHeap) Update(item string, fingerprint uint32, count uint32) {
	if count < h.Min() && h.Full() { // not in top k: ignore
		return
	}

	if i := h.Find(item); i >= 0 { // already in heap: update count
		h.Items[i].Count = count
		heap.Fix(h, i)
		return
	}

	h.StoredKeysBytes += len(item)

	if !h.Full() { // heap not full: add to heap
		h.Push(HeapItem{
			Count:       count,
			Fingerprint: fingerprint,
			Item:        item,
		})
		return
	}

	// replace min on heap
	minItem := h.Items[0].Item
	h.StoredKeysBytes -= len(minItem)
	delete(h.Index, minItem)
	h.Items[0] = HeapItem{
		Count:       count,
		Fingerprint: fingerprint,
		Item:        item,
	}
	h.Index[item] = 0
	heap.Fix(h, 0)
}
