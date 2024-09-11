# topk

[![Go Reference](https://pkg.go.dev/badge/github.com/keilerkonzept/topk.svg)](https://pkg.go.dev/github.com/keilerkonzept/topk)

Sliding-window and regular top-K sketches.

- A fast implementation of the HeavyKeeper top-K sketch inspired by the [segmentio implementation](github.com/segmentio/topk) and [RedisBloom implementation](https://github.com/RedisBloom/RedisBloom/blob/b5916e1b9fba17829c3e329c127b99d706eb31f6/src/topk.c).
- A sliding-window top-K sketch, also based on HeavyKeeper, as described in ["A Sketch Framework for Approximate Data Stream Processing in Sliding Windows"](https://yangtonghome.github.io/uploads/SlidingSketch_TKDE2022_final.pdf)

```go
import (
	"github.com/keilerkonzept/topk" // plain sketch
	"github.com/keilerkonzept/topk/sliding" // sliding-window sketch
)
```

## Contents

- [Examples](#examples)
    - [Top-K Sketch](#top-k-sketch)
    - [Sliding-window Top-K Sketch](#sliding-window-top-k-sketch)
- [Benchmarks](#benchmarks)
    - [Top-K Sketch](#top-k-sketch)
    - [Sliding-Window Top-K Sketch](#sliding-window-top-k-sketch)
    - [Decay LUT impact](#decay-lut-impact)

## Examples

### Top-K Sketch

```go
package main

import (
	"log"
	"github.com/keilerkonzept/topk"
)

func main() {
	// make a new sketch keeping track of k=3 items using 1024x3 = 3072 buckets.
	sketch := topk.New(3, topk.WithWidth(1024), topk.WithDepth(3))

	log.Println("the sketch takes up", sketch.SizeBytes(), "bytes in memory")

	sketch.Incr("an item")            // count "an item" 1 time
	sketch.Add("an item", 123)        // count "an item" 123 times
	sketch.Add("another item", 4)     // count "another item" 4 times
	sketch.Add("an item", 5)          // count "an item" 5 more times
	sketch.Add("yet another item", 6) // count "yet another item" 6 times

	if sketch.Query("an item") {
		// "an item" is in the top K items observed within the last 60 ticks
	}

	_ = sketch.Count("another item") // return the estimated count for "another item"

	// SortedSlice() returns the current top-K entries as a slice of {Fingerprint,Item,Count} structs.
	for _, entry := range sketch.SortedSlice() {
		log.Println(entry.Item, "has been counted", entry.Count, "times")
	}

	// Iter is an interator over the (*not* sorted) current top-K entries.
	for entry := range sketch.Iter {
		log.Println(entry.Item, "has been counted", entry.Count, "times")
	}
	sketch.Reset() // reset to New() state
}
```


### Sliding-window Top-K Sketch

```go
package main

import (
	"log"
	"github.com/keilerkonzept/topk/sliding"
)

func main() {
	// make a new sketch keeping track of k=3 items over a window of the last 60 ticks
	// use width=1024 x depth=3 = 3072 buckets
	sketch := sliding.New(3, 60, sliding.WithWidth(1024), sliding.WithDepth(3))

	log.Println("the sketch takes up", sketch.SizeBytes(), "bytes in memory")

	sketch.Incr("an item")            // count "an item" 1 time
	sketch.Add("an item", 123)        // count "an item" 123 times
	sketch.Tick()                     // advance time by one tick
	sketch.Add("another item", 4)     // count "another item" 4 times
	sketch.Ticks(2)                   // advance time by two ticks
	sketch.Add("an item", 5)          // count "an item" 5 more times
	sketch.Add("yet another item", 6) // count "yet another item" 6 times

	if sketch.Query("an item") {
		// "an item" is in the top K items observed within the last 60 ticks
	}

	_ = sketch.Count("another item") // return the estimated count for "another item"

	// SortedSlice() returns the current top-K entries as a slice of {Fingerprint,Item,Count} structs.
	for _, entry := range sketch.SortedSlice() {
		log.Println(entry.Item, "has been counted", entry.Count, "times")
	}

	// Iter is an interator over the (*not* sorted) current top-K entries.
	for entry := range sketch.Iter {
		log.Println(entry.Item, "has been counted", entry.Count, "times")
	}
	sketch.Reset() // reset to New() state
}
```

## Benchmarks

### Top-K Sketch

(TBD)

### Sliding-Window Top-K Sketch

(TBD)

### Decay LUT impact

The size of the look-up table is configured using the `WithDecayLUTSize` option. If the look-up table covers the actual counts involved, the speedup can be significant:

```
goos: darwin
goarch: arm64
pkg: github.com/keilerkonzept/topk
cpu: Apple M1 Pro

BenchmarkSketch_1000_3k_3-10           	  124646	      8450 ns/op
BenchmarkSegmentioTopK_1000_3k_3-10    	   39903	     32345 ns/op
```
