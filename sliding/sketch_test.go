package sliding_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/internal/sizeof"
	"github.com/keilerkonzept/topk/sliding"
)

func TestNewSketch_DefaultParameters(t *testing.T) {
	k := 10
	sketch := sliding.New(k, 3)

	if sketch.K != k {
		t.Errorf("Expected K = %d, got %d", k, sketch.K)
	}
	if sketch.WindowSize != 3 {
		t.Errorf("Expected WindowSize = 3, got %d", sketch.WindowSize)
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

func TestSketch_SizeBytes(t *testing.T) {
	k := 10
	sketch := sliding.New(k, 10)

	// Estimate the size
	size := sketch.SizeBytes()
	if size <= 0 {
		t.Errorf("Expected sketch size to be positive, got %d", size)
	}
	if size <= sketch.Width*sketch.Depth*sizeof.UInt32*(1+sketch.BucketHistoryLength) {
		t.Errorf("Expected sketch size to be at least as large as the width*depth* counters+fingerprints, got %d", size)
	}
}

func TestNewSketch_WithOptions(t *testing.T) {
	k := 10
	sketch := sliding.New(k, 3, sliding.WithDepth(5), sliding.WithWidth(300), sliding.WithDecay(0.8), sliding.WithDecayLUTSize(1024), sliding.WithBucketHistoryLength(3))

	// Verify the options
	if sketch.WindowSize != 3 {
		t.Errorf("Expected WindowSize = 3, got %d", sketch.WindowSize)
	}
	if sketch.Depth != 5 {
		t.Errorf("Expected Depth = 5, got %d", sketch.Depth)
	}
	if sketch.BucketHistoryLength != 3 {
		t.Errorf("Expected BucketHistoryLength = 3, got %d", sketch.BucketHistoryLength)
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

func TestSketchTopKSimple(t *testing.T) {
	sketch := sliding.New(3, 10)

	// Add items X, Y, Z
	sketch.Add("X", 5)
	sketch.Add("Y", 3)
	sketch.Add("Z", 2)
	sketch.Incr("Y")

	expected := []heap.Item{
		{topk.Fingerprint("X"), "X", 5},
		{topk.Fingerprint("Y"), "Y", 4},
		{topk.Fingerprint("Z"), "Z", 2},
	}
	actual := sketch.SortedSlice()
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}
	for _, item := range expected {
		if !sketch.Query(item.Item) {
			t.Errorf("Expected item %q to be in the top-K set, but it is not.", item.Item)
		}
	}
	for _, item := range expected {
		actualCount := sketch.Count(item.Item)
		expectedCount := item.Count
		if actualCount != expectedCount {
			t.Errorf("Expected Count(%s) = %d, got %d", item.Item, expectedCount, actualCount)
		}
	}
}

func TestSketchSlidingWindowDecay(t *testing.T) {
	sketch := sliding.New(2, 2)

	// Add items X, Y, Z at time t = 0
	sketch.Add("X", 3)
	sketch.Add("Y", 2)
	sketch.Add("Z", 1)

	// Check top-K after adding
	expected := []heap.Item{
		{topk.Fingerprint("X"), "X", 3},
		{topk.Fingerprint("Y"), "Y", 2},
	}
	actual := sketch.SortedSlice()
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}

	// Simulate time progression (advance window)
	sketch.Tick() // t = 1
	sketch.Tick() // t = 2

	// Add more counts for Y and Z at t = 2
	sketch.Add("Y", 2)
	sketch.Add("Z", 3)

	// Check updated top-K
	expected = []heap.Item{
		{topk.Fingerprint("Z"), "Z", 3},
		{topk.Fingerprint("Y"), "Y", 2},
	}
	actual = sketch.SortedSlice()
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}
}

func TestSketchTopKSliding(t *testing.T) {
	sketch := sliding.New(2, 2, sliding.WithWidth(10), sliding.WithDepth(2), sliding.WithBucketHistoryLength(2))

	//t  0
	//
	//X  3
	//Y  2
	//Z  1
	// [ _ _ ] {x:3,y:2}+
	sketch.Add("X", 3)
	sketch.Add("Y", 2)
	sketch.Add("Z", 1)
	{
		expected := []heap.Item{
			{topk.Fingerprint("X"), "X", 3},
			{topk.Fingerprint("Y"), "Y", 2},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}
	sketch.Tick()

	//t  0 1
	//
	//X  3 2
	//Y  2 2
	//Z  1 1
	// [ _ _ ]   {x:5,y:4}
	sketch.Add("X", 2)
	sketch.Add("Y", 2)
	sketch.Add("Z", 1)
	{
		expected := []heap.Item{
			{topk.Fingerprint("X"), "X", 5},
			{topk.Fingerprint("Y"), "Y", 4},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}
	sketch.Tick()

	//t  0 1 2
	//
	//X  3 2 0
	//Y  2 2 1
	//Z  1 1 3
	// [ _ _ ]  {x:5,y:4}
	//   [ _ _ ] {z:4,y:3}
	sketch.Add("Y", 1)
	sketch.Add("Z", 3)
	{
		expected := []heap.Item{
			{topk.Fingerprint("Z"), "Z", 4},
			{topk.Fingerprint("Y"), "Y", 3},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}
	sketch.Tick()

	//t  0 1 2 3
	//
	//X  3 2 0 0
	//Y  2 2 1 1
	//Z  1 1 3 3
	// [ _ _ ]  {x:5,y:4}
	//   [ _ _ ] {z:4,y:3}
	//     [ _ _ ] {z:6:y:2}
	sketch.Add("Y", 1)
	sketch.Add("Z", 3)
	{
		expected := []heap.Item{
			{topk.Fingerprint("Z"), "Z", 6},
			{topk.Fingerprint("Y"), "Y", 2},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}

	sketch.Tick()
	//t  0 1 2 3 4
	//
	//X  3 2 0 0 0
	//Y  2 2 1 1 0
	//Z  1 1 3 3 0
	// [ _ _ ]  {x:5,y:4}
	//   [ _ _ ] {z:4,y:3}
	//     [ _ _ ] {z:6:y:2}
	//       [ _ _ ] {z:3:y:1}
	{
		expected := []heap.Item{
			{topk.Fingerprint("Z"), "Z", 3},
			{topk.Fingerprint("Y"), "Y", 1},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}

	sketch.Tick()
	sketch.Add("X", 1)
	//t  0 1 2 3 4 5
	//
	//X  3 2 0 0 0 1
	//Y  2 2 1 1 0 0
	//Z  1 1 3 3 0 0
	// [ _ _ ]  {x:5,y:4}
	//   [ _ _ ] {z:4,y:3}
	//     [ _ _ ] {z:6:y:2}
	//       [ _ _ ] {z:3:y:1}
	//         [ _ _ ] {x:1}
	{
		expected := []heap.Item{
			{topk.Fingerprint("X"), "X", 1},
		}
		actual := sketch.SortedSlice()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}
}

func TestSketch_Iter(t *testing.T) {
	k := 3
	sketch := sliding.New(k, 3)
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
	sketch := sliding.New(k, 3)

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
	windowSize := 3
	decay := 0.9
	width := 256
	depth := 1
	noiseItems := 1000
	noiseItemsFrequency := []int{2000, 2000, 2000, 0, 0, 0}

	sketch := sliding.New(K, windowSize, sliding.WithWidth(width), sliding.WithDepth(depth), sliding.WithDecay(float32(decay)))

	testCases := []struct {
		item      string
		increment []uint32
		total     []uint32
	}{
		{"high_freq", []uint32{500, 500, 500, 0, 0, 0}, []uint32{500, 1000, 1500, 1000, 500, 0}},
		{"medium_freq", []uint32{100, 200, 300, 0, 0, 0}, []uint32{100, 300, 600, 500, 300, 0}},
		{"low_freq", []uint32{50, 50, 100, 0, 0, 0}, []uint32{50, 100, 200, 150, 100, 0}},
	}

	// 6 time steps
	for tick := range 6 {
		sketch.Tick()

		// Insert test items
		for _, tc := range testCases {
			sketch.Add(tc.item, tc.increment[tick])
		}

		// Insert noise items, decaying the test items' counters on collisions.'
		for j := 0; j < noiseItemsFrequency[tick]; j++ {
			noiseItem := fmt.Sprintf("noise_item_%d", rand.IntN(noiseItems))
			sketch.Incr(noiseItem)
		}

		for _, tc := range testCases {
			actualCount := sketch.Count(tc.item)

			// TODO: figure out an approximation for the lower bound as well
			if actualCount > tc.total[tick] {
				t.Errorf("tick %d: Count for %s should be less than or equal to the precise count (only under-estimation errors should occur). Expected <=%v, actual: %v", tick, tc.item, tc.increment[tick], actualCount)
			}
		}
	}
}
