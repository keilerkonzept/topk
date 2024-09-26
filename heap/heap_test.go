package heap_test

import (
	"testing"
	"unsafe"

	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/internal/sizeof"
)

func TestMinHeap_LessSwap(t *testing.T) {
	h := heap.NewMin(5)

	h.Items = []heap.Item{
		{Item: "a", Count: 5, Fingerprint: 1},
		{Item: "b", Count: 2, Fingerprint: 2},
		{Item: "c", Count: 3, Fingerprint: 3},
	}

	// Check Less function
	if !h.Less(1, 0) {
		t.Errorf("expected Less(1, 0) to be true")
	}
	if h.Less(0, 1) {
		t.Errorf("expected Less(0, 1) to be false")
	}

	// Check Swap function
	h.Swap(0, 1)
	if h.Items[0].Item != "b" || h.Items[1].Item != "a" {
		t.Errorf("expected Swap to switch elements 'a' and 'b'")
	}
}

func TestMinHeap_Full(t *testing.T) {
	h := heap.NewMin(2)

	h.Update("a", 1, 2)
	if h.Full() {
		t.Errorf("expected heap to not be full")
	}

	h.Update("b", 2, 2)
	if !h.Full() {
		t.Errorf("expected heap to be full")
	}
}

func TestMinHeap_Update(t *testing.T) {
	h := heap.NewMin(2)

	// Insert new item
	h.Update("a", 1, 10)
	if h.Items[0].Item != "a" {
		t.Errorf("expected 'a' to be in the heap")
	}

	// Insert more items
	h.Update("b", 2, 5)
	h.Update("c", 3, 8)
	h.Update("d", 3, 1)

	// "b" should be removed as it has the lowest count and heap is full
	if h.Contains("b") {
		t.Errorf("expected 'b' to be removed from the heap")
	}
	// "d" should not enter the heap as its count is less than Min()
	if h.Contains("d") {
		t.Errorf("expected 'd' to never enter the heap")
	}

	// Update an existing item
	h.Update("c", 3, 15)
	if h.Items[0].Item != "a" || h.Items[1].Item != "c" {
		t.Errorf("expected 'a' and 'c' to be in the heap after update")
	}
}

func TestMinHeap_Min(t *testing.T) {
	h := heap.NewMin(2)

	// Empty heap
	if h.Min() != 0 {
		t.Errorf("expected Min to return 0 for empty heap")
	}

	// Push some items and check minimum
	h.Update("a", 1, 10)
	h.Update("b", 2, 5)
	h.Update("c", 2, 3)

	if h.Min() != 5 {
		t.Errorf("expected Min to return 5, got %d", h.Min())
	}
}

func TestMinHeap_Reinit(t *testing.T) {
	h := heap.NewMin(3)

	h.Push(heap.Item{Item: "a", Count: 0, Fingerprint: 1})
	h.Push(heap.Item{Item: "b", Count: 2, Fingerprint: 2})
	h.Push(heap.Item{Item: "c", Count: 3, Fingerprint: 3})

	// Reinit should remove items with 0 count
	h.Reinit()
	if h.Len() != 2 {
		t.Errorf("expected Len after Reinit to be 2, got %d", h.Len())
	}
	if h.Contains("a") {
		t.Errorf("expected 'a' to be removed from the heap")
	}
}

func TestMinHeap_Find(t *testing.T) {
	h := heap.NewMin(3)
	h.Update("a", 1, 10)

	// Find existing item
	idx := h.Find("a")
	if idx != 0 {
		t.Errorf("expected 'a' to be at index 0, got %d", idx)
	}

	// Find non-existing item
	idx = h.Find("b")
	if idx != -1 {
		t.Errorf("expected 'b' to not be found, got %d", idx)
	}
}

func TestMinHeap_Get(t *testing.T) {
	h := heap.NewMin(3)
	h.Update("a", 1, 10)

	// Get existing item
	item := h.Get("a")
	if item == nil || item.Item != "a" {
		t.Errorf("expected to get item 'a', got '%v'", item)
	}

	// Get non-existing item
	item = h.Get("b")
	if item != nil {
		t.Errorf("expected to get nil for non-existing item, got '%v'", item)
	}
}

func TestMinHeap_SizeBytes(t *testing.T) {
	h := heap.NewMin(3)

	const (
		sizeofMinStruct = int(unsafe.Sizeof(heap.Min{}))
		sizeofItem      = int(unsafe.Sizeof(heap.Item{}))
	)

	// Initial size should only account for the struct and empty containers
	expectedSize := sizeofMinStruct + 3*sizeofItem + sizeof.StringIntMap
	if h.SizeBytes() != expectedSize {
		t.Errorf("expected SizeBytes to be %d, got %d", expectedSize, h.SizeBytes())
	}

	h.Update("a", 1, 5)
	expectedSize += len("a") + sizeof.String + sizeof.Int // Size of new item in heap
	if h.SizeBytes() != expectedSize {
		t.Errorf("expected SizeBytes to be %d, got %d", expectedSize, h.SizeBytes())
	}

	h.Update("b", 2, 10)
	expectedSize += len("b") + sizeof.String + sizeof.Int
	if h.SizeBytes() != expectedSize {
		t.Errorf("expected SizeBytes to be %d, got %d", expectedSize, h.SizeBytes())
	}

	h.Update("long_string_item", 3, 15)
	expectedSize += len("long_string_item") + sizeof.String + sizeof.Int
	if h.SizeBytes() != expectedSize {
		t.Errorf("expected SizeBytes to be %d, got %d", expectedSize, h.SizeBytes())
	}
}

func TestMin_Reset(t *testing.T) {
	// Create a new Min heap with capacity 3
	minHeap := heap.NewMin(3)

	// Add some items to the heap
	minHeap.Update("item1", 12345, 10)
	minHeap.Update("item2", 12346, 20)
	minHeap.Update("item3", 12347, 5)

	// Verify that the heap contains 3 items
	if len(minHeap.Items) != 3 {
		t.Fatalf("expected heap length 3, got %d", len(minHeap.Items))
	}

	// Verify that the index map contains 3 items
	if len(minHeap.Index) != 3 {
		t.Fatalf("expected index length 3, got %d", len(minHeap.Index))
	}

	// Verify StoredKeysBytes is updated correctly
	expectedBytes := len("item1") + len("item2") + len("item3")
	if minHeap.StoredKeysBytes != expectedBytes {
		t.Fatalf("expected StoredKeysBytes %d, got %d", expectedBytes, minHeap.StoredKeysBytes)
	}

	// Call Reset on the heap
	minHeap.Reset()

	// Verify that the heap is empty
	if len(minHeap.Items) != 0 {
		t.Fatalf("expected heap length 0 after reset, got %d", len(minHeap.Items))
	}

	// Verify that the index map is empty
	if len(minHeap.Index) != 0 {
		t.Fatalf("expected index length 0 after reset, got %d", len(minHeap.Index))
	}

	// Verify StoredKeysBytes is reset to 0
	if minHeap.StoredKeysBytes != 0 {
		t.Fatalf("expected StoredKeysBytes 0 after reset, got %d", minHeap.StoredKeysBytes)
	}
}
