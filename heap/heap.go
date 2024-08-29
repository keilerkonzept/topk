package heap

import (
	"container/heap"

	"github.com/keilerkonzept/topk/internal/sizeof"
)

type Item struct {
	Fingerprint uint32
	Item        string
	Count       uint32
}

type Min struct {
	Items           []Item
	Index           map[string]int
	StoredKeysBytes int
}

func NewMin(k int) *Min {
	return &Min{
		Items: make([]Item, k),
		Index: make(map[string]int, k),
	}
}

var _ heap.Interface = &Min{}

func (me Min) SizeBytes() int {
	structSize := sizeofMinStruct
	bucketsSize := len(me.Items)*sizeofItem + me.StoredKeysBytes
	indexSize := sizeof.StringIntMap + (sizeof.Int+sizeof.String)*len(me.Index)
	return structSize + bucketsSize + indexSize
}

func (me *Min) Reinit() {
	heap.Init(me)
	for me.Len() > 0 && me.Items[0].Count == 0 {
		item := me.Items[0].Item
		heap.Pop(me)
		delete(me.Index, item)
	}
}

func (me Min) Full() bool { return len(me.Items) == cap(me.Items) }

// Len is container/heap.Interface.Len().
func (me Min) Len() int { return len(me.Items) }

// Less is container/heap.Interface.Less().
func (me Min) Less(i, j int) bool {
	ic := me.Items[i].Count
	jc := me.Items[j].Count
	if ic == jc {
		return me.Items[i].Item < me.Items[j].Item
	}
	return ic < jc
}

// Swap is container/heap.Interface.Swap().
func (me Min) Swap(i, j int) {
	itemi := me.Items[i].Item
	itemj := me.Items[j].Item
	me.Items[i], me.Items[j] = me.Items[j], me.Items[i]
	me.Index[itemi] = j
	me.Index[itemj] = i
}

// Push is container/heap.Interface.Push().
func (me *Min) Push(x interface{}) {
	b := x.(Item)
	me.Items = append(me.Items, b)
	me.Index[b.Item] = len(me.Items) - 1
}

// Pop is container/heap.Interface.Pop().
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

func (me Min) Find(item string) (i int) {
	if i, ok := me.Index[item]; ok {
		return i
	}
	return -1
}

func (me Min) Contains(item string) bool {
	_, ok := me.Index[item]
	return ok
}

func (me Min) Get(item string) *Item {
	if i, ok := me.Index[item]; ok {
		return &me.Items[i]
	}
	return nil
}

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
		me.Push(Item{
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
