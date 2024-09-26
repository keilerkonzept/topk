package topk_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/internal/sizeof"
	segmentiotopk "github.com/segmentio/topk"
)

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

func TestSketchCollisions(t *testing.T) {
	K := 3
	decay := 0.9
	for _, width := range []int{4, 8} {
		for _, depth := range []int{1} {
			t.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d", K, depth, width), func(t *testing.T) {
				noiseItems := 100
				noiseItemsFrequency := 1000
				sketch := topk.New(K, topk.WithWidth(width), topk.WithDepth(depth), topk.WithDecay(float32(decay)))

				testCases := []struct {
					item  string
					count uint32
				}{
					{"a", 50},
					{"b", 40},
					{"c", 30},
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
					noiseItem := fmt.Sprintf("n%d", i)
					sketch.Add(noiseItem, uint32(noiseItemsFrequency))
				}

				for _, tc := range testCases {
					if sketch.Query(tc.item) {
						t.Errorf("item %s should not be in the top-k", tc.item)
					}
				}
			})
		}
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

	noiseInTopK := 0
	for i := 0; i < noiseItems; i++ {
		noiseItem := fmt.Sprintf("noise_item_%d", i)
		actualCount := sketch.Count(noiseItem)
		actualTop := sketch.Query(noiseItem)
		if actualCount > uint32(noiseItemsFrequency) {
			t.Errorf("%s, %v > %v", noiseItem, actualCount, noiseItemsFrequency)
		}
		if actualTop {
			noiseInTopK++
		}
	}
	maxNoiseInTopK := K - len(testCases)
	if noiseInTopK > K-len(testCases) {
		t.Errorf("no more than %d noise items should be in the top K, got %d", maxNoiseInTopK, noiseInTopK)
	}
	for _, tc := range testCases {
		actualCount := sketch.Count(tc.item)

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

func TestSketchVsSegmentio(t *testing.T) {
	K := 3
	decay := 0.9
	width := max(256, int(float64(K)*math.Log(float64(K))))
	depth := max(3, int(math.Log(float64(K))))
	noiseItems := 10_000
	noiseItemsFrequency := 50
	approxErrorProbability := 1.0

	sketch := topk.New(K, topk.WithWidth(width), topk.WithDepth(depth), topk.WithDecay(float32(decay)))
	segmentiosketch := segmentiotopk.New(K, decay)

	testCases := []struct {
		item    string
		count   uint32
		notTopK bool
	}{
		{"high_freq", 1000, false},
		{"medium_freq", 500, false},
		{"low_freq", 100, false},
	}

	totalItems := noiseItems * noiseItemsFrequency
	for _, tc := range testCases {
		totalItems += int(tc.count)
	}

	// Insert test items
	for _, tc := range testCases {
		sketch.Add(tc.item, tc.count)
		segmentiosketch.Sample(tc.item, tc.count)
	}

	// Insert noise items, decaying the test items' counters on collisions.'
	for i := 0; i < noiseItems; i++ {
		noiseItem := fmt.Sprintf("noise_item_%d", i)
		sketch.Add(noiseItem, uint32(noiseItemsFrequency))
		segmentiosketch.Sample(noiseItem, uint32(noiseItemsFrequency))
	}

	for _, tc := range testCases {
		actualCount := sketch.Count(tc.item)
		actualTop := sketch.Query(tc.item)
		actualCountSegmentio, actualTopSegmentio := segmentiosketch.Count(tc.item)

		epsilon := 1 / (approxErrorProbability * (float64(width*depth) * float64(tc.count) * float64(1-decay)))

		lowerBound := float64(tc.count) - math.Ceil(epsilon*float64(totalItems-int(tc.count)))
		if lowerBound < 0 {
			lowerBound = 0
		}

		if actualTop != actualTopSegmentio {
			t.Errorf("top-k query for %s in segmentio/topk = %v != %v = top-k query here: theoretical bound >=%v, segmentio/topk: %v, actual: %v", tc.item, actualTopSegmentio, actualTop, tc.count, actualCountSegmentio, actualCount)
			t.Fatalf("%#v %#v", sketch.SortedSlice(), segmentiosketch.Top())
		}
		if actualCount != actualCountSegmentio {
			t.Fatalf("Count for %s differs from segmentio/topk: theoretical bound >=%v, segmentio/topk: %v, actual: %v", tc.item, tc.count, actualCountSegmentio, actualCount)
		}
		if actualCount > tc.count {
			t.Fatalf("Count for %s should be less than or equal to the precise count (only under-estimation errors should occur). Expected >=%v, actual: %v", tc.item, tc.count, actualCount)
		}
		if actualCount < uint32(lowerBound) {
			t.Fatalf("Count for %s should be greater than or equal to the estimated decayed count. Expected >=%v, actual: %v", tc.item, lowerBound, actualCount)
		}
		if actualCountSegmentio > tc.count {
			t.Fatalf("segmentio/topk count for %s should be less than or equal to the precise count (only under-estimation errors should occur). Expected >=%v, actual: %v", tc.item, tc.count, actualCountSegmentio)
		}
		if actualCountSegmentio < uint32(lowerBound) {
			t.Fatalf("segmentio/topk count for %s should be greater than or equal to the estimated decayed count. Expected >=%v, actual: %v", tc.item, lowerBound, actualCountSegmentio)
		}
	}
}
