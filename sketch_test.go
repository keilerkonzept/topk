package topk_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/internal/sizeof"
	segmentio_topk "github.com/segmentio/topk"
)

func BenchmarkSketch_1000_3k_3(b *testing.B) {
	sketch := topk.New(1000, topk.WithWidth(3_000), topk.WithDepth(3), topk.WithDecayLUTSize(1024))
	items := make([]string, 2048)
	for i := range items {
		items[i] = fmt.Sprint(i)
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		sketch.Add(items[i%len(items)], uint32(i%len(items)))
	}
}

func BenchmarkSegmentioTopK_1000_3k_3(b *testing.B) {
	sketch := segmentio_topk.New(1000, 0.9)
	items := make([]string, 2048)
	for i := range items {
		items[i] = fmt.Sprint(i)
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		sketch.Sample(items[i%len(items)], uint32(i%len(items)))
	}
}

func TestNewSketch_DefaultParameters(t *testing.T) {
	k := 10
	sketch := topk.New(k)

	if sketch.K != k {
		t.Errorf("Expected K = %d, got %d", k, sketch.K)
	}
	if sketch.Width <= 0 {
		t.Errorf("Width should be positive, got %d", sketch.Width)
	}
	if sketch.Depth <= 0 {
		t.Errorf("Depth should be positive, got %d", sketch.Depth)
	}
	if sketch.Decay != 0.9 {
		t.Errorf("Expected default decay = 0.9, got %f", sketch.Decay)
	}
	if len(sketch.DecayLUT) == 0 {
		t.Error("Expected non-empty decay LUT, got empty slice")
	}
}

func TestNewSketch_WithOptions(t *testing.T) {
	k := 10
	sketch := topk.New(k, topk.WithDepth(5), topk.WithWidth(300), topk.WithDecay(0.8), topk.WithDecayLUTSize(1024))

	// Verify the options
	if sketch.Depth != 5 {
		t.Errorf("Expected Depth = 5, got %d", sketch.Depth)
	}
	if sketch.Width != 300 {
		t.Errorf("Expected Width = 300, got %d", sketch.Width)
	}
	if sketch.Decay != 0.8 {
		t.Errorf("Expected Decay = 0.8, got %f", sketch.Decay)
	}
	if len(sketch.DecayLUT) != 1024 {
		t.Errorf("Expected Decay LUT size = 1024, got %d", len(sketch.DecayLUT))
	}
}

func TestSketch_SizeBytes(t *testing.T) {
	k := 10
	sketch := topk.New(k)

	// Estimate the size
	size := sketch.SizeBytes()
	if size <= 0 {
		t.Errorf("Expected sketch size to be positive, got %d", size)
	}
	if size <= sketch.Width*sketch.Depth*2*sizeof.UInt32 {
		t.Errorf("Expected sketch size to be at least as large as the width*depth counters+fingerprints, got %d", size)
	}
}

func TestSketch_AddIncrQuery(t *testing.T) {
	k := 3
	sketch := topk.New(k)
	item := "item1"

	// Increment and check count
	sketch.Incr(item)
	count := sketch.Count(item)
	if count != 1 {
		t.Errorf("Expected count = 1 for item %s, got %d", item, count)
	}

	// Add more and verify top-K
	sketch.Add(item, 5)

	if !sketch.Query(item) {
		t.Errorf("Expected item %s to be in the top-K", item)
	}
}

func TestSketch_SortedSlice(t *testing.T) {
	k := 3
	sketch := topk.New(k)

	items := []string{"item1", "item2", "item3", "item4"}
	for i, item := range items {
		sketch.Add(item, uint32(i))
	}

	topK := sketch.SortedSlice()

	// Verify the top-K slice
	if len(topK) != k {
		t.Errorf("Expected top-K size = %d, got %d", k, len(topK))
	}

	// Check if the items are ordered correctly by count
	expectedOrder := []string{"item4", "item3", "item2"}
	for i, item := range expectedOrder {
		if topK[i].Item != item {
			t.Errorf("Expected item %s at position %d, got %s", item, i, topK[i].Item)
		}
	}
}

func TestSketch_Iter(t *testing.T) {
	k := 3
	sketch := topk.New(k)
	for entry := range sketch.Iter {
		t.Errorf("Unexpected entry in top-K iteration over empty sketch = %#v", entry)
	}

	items := []string{"item1", "item2", "item3", "item4"}
	for i, item := range items {
		sketch.Add(item, uint32(i))
	}

	once := false
	sketch.Iter(func(_ *heap.Item) bool {
		if once {
			t.Error("Iteration should stop after first element")
		}
		once = true
		return false
	})

	// Check if the items are ordered correctly by count
	expectedEntries := map[string]struct{}{"item4": {}, "item3": {}, "item2": {}}
	for entry := range sketch.Iter {
		if _, ok := expectedEntries[entry.Item]; !ok {
			t.Errorf("Unexpected entry in top-K iteration = %#v", entry)
		}
		delete(expectedEntries, entry.Item)
	}
	if len(expectedEntries) != 0 {
		t.Errorf("Expected entry in top-K iteration, but did not encounter them = %#v", expectedEntries)

	}
}

func TestSketch_Reset(t *testing.T) {
	k := 3
	sketch := topk.New(k)

	// Add some items
	sketch.Incr("item1")
	sketch.Incr("item2")

	// Reset the sketch
	sketch.Reset()

	// Verify reset state
	if sketch.Count("item1") != 0 {
		t.Errorf("Expected count = 0 after reset, got %d", sketch.Count("item1"))
	}
	if len(sketch.SortedSlice()) != 0 {
		t.Errorf("Expected no items in top-K after reset")
	}
}

func TestSketchErrorBounds(t *testing.T) {
	K := 10
	decay := 0.9
	width := 32
	depth := 1
	noiseItems := 1_000
	noiseItemsFrequency := 50
	approxErrorProbability := 1.0

	sketch := topk.New(K, topk.WithWidth(width), topk.WithDepth(depth), topk.WithDecay(float32(decay)))

	testCases := []struct {
		item  string
		count uint32
	}{
		{"high_freq", 1000},
		{"medium_freq", 500},
		{"low_freq", 100},
	}

	totalItems := noiseItems * noiseItemsFrequency
	for _, tc := range testCases {
		totalItems += int(tc.count)
	}

	// Insert test items
	for _, tc := range testCases {
		sketch.Add(tc.item, tc.count)
	}

	// Insert noise items, decaying the test items' counters on collisions.'
	for i := 0; i < noiseItems; i++ {
		noiseItem := fmt.Sprintf("noise_item_%d", i)
		sketch.Add(noiseItem, uint32(noiseItemsFrequency))
	}

	for _, tc := range testCases {
		actualCount := sketch.Count(tc.item)

		// prob = 1/(epsilon*width*count*(1-b))
		// (epsilon*width*count*(1-b))*prob = 1
		// epsilon = 1/(width*count*(1-b))*prob)
		epsilon := 1 / (approxErrorProbability * (float64(width*depth) * float64(tc.count) * float64(1-decay)))

		lowerBound := float64(tc.count) - math.Ceil(epsilon*float64(totalItems-int(tc.count)))
		if lowerBound < 0 {
			lowerBound = 0
		}

		if actualCount > tc.count {
			t.Fatalf("Count for %s should be less than or equal to the precise count (only under-estimation errors should occur). Expected >=%v, actual: %v", tc.item, tc.count, actualCount)
		}
		if actualCount < uint32(lowerBound) {
			t.Fatalf("Count for %s should be greater than or equal to the estimated decayed count. Expected >=%v, actual: %v", tc.item, lowerBound, actualCount)
		}
	}
}
