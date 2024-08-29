package topk_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	topk "github.com/keilerkonzept/sliding-topk"
)

func TestSketch(t *testing.T) {
	sketch := topk.New(2, 2, topk.WithWidth(10), topk.WithDepth(2), topk.WithBucketHistoryLength(2))

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
		expected := []topk.ItemWithCount{
			{"X", 3},
			{"Y", 2},
		}
		actual := sketch.TopK()
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
		expected := []topk.ItemWithCount{
			{"X", 5},
			{"Y", 4},
		}
		actual := sketch.TopK()
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
		expected := []topk.ItemWithCount{
			{"Z", 4},
			{"Y", 3},
		}
		actual := sketch.TopK()
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
		expected := []topk.ItemWithCount{
			{"Z", 6},
			{"Y", 2},
		}
		actual := sketch.TopK()
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
		expected := []topk.ItemWithCount{
			{"Z", 3},
			{"Y", 1},
		}
		actual := sketch.TopK()
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
		expected := []topk.ItemWithCount{
			{"X", 1},
		}
		actual := sketch.TopK()
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	}
}
