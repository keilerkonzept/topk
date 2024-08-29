package sliding_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/sliding"
)

func TestSketch(t *testing.T) {
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
