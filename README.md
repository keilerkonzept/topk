# sliding-topk

Sliding HeavyKeeper, as described in ["A Sketch Framework for Approximate Data Stream Processing in Sliding Windows"](https://yangtonghome.github.io/uploads/SlidingSketch_TKDE2022_final.pdf)

```go
import (
	topk "github.com/keilerkonzept/sliding-topk"
)

func main() {
	// make a new sketch keeping track of k=3 items over a window of the last 60 ticks
	// use width=1024 x depth=3 = 3072 buckets
	sketch := topk.New(3, 60, topk.WithWidth(1024),topk.WithDepth(3))

	log.Println("the sketch takes", sketch.SizeBytes(), "bytes in memory")

	sketch.Incr("an item") // count "an item" 1 time
	sketch.Add("an item", 123) // count "an item" 123 times
	sketch.Tick(1) // advance time by one tick
	sketch.Add("another item", 4) // count "another item" 4 times
	sketch.Tick(2) // advance time by two ticks
	sketch.Add("an item", 5) // count "an item" 5 more times
	sketch.Add("yet another item", 6) // count "yet another item" 6 times

	if sketch.Query("an item") {
		// "an item" is in the top K items observed within the last 60 ticks
	}

	_ = sketch.Count("another item") // return the estimated count for "another item"

	for entry := range sketch.TopK() {// TopK() rseturn all top K items as a slice of {Item,Count} structs
		log.Println(entry.Item, "counted", entry.Count, "times")
	}

	sketch.Reset() // reset to New() state
}
