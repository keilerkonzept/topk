# topk
[![Coverage](https://img.shields.io/badge/Coverage-97.1%25-brightgreen)](https://github.com/keilerkonzept/topk/actions/workflows/gocover.yaml)

[![Go Reference](https://pkg.go.dev/badge/github.com/keilerkonzept/topk.svg)](https://pkg.go.dev/github.com/keilerkonzept/topk)
[![Go Report Card](https://goreportcard.com/badge/github.com/keilerkonzept/topk)](https://goreportcard.com/report/github.com/keilerkonzept/topk)

Sliding-window and regular top-K sketches.

- A fast implementation of the [**HeavyKeeper top-K sketch**](https://www.usenix.org/conference/atc18/presentation/gong) inspired by the [segmentio implementation](https://github.com/segmentio/topk) and [RedisBloom implementation](https://github.com/RedisBloom/RedisBloom/blob/b5916e1b9fba17829c3e329c127b99d706eb31f6/src/topk.c). [Significantly faster (~1.5x)](#comparison-with-segmentiotopk) than [segmentio/topk](https://github.com/segmentio/topk) on small sketches (k <= 1000) and [much faster (10x-90x)](#comparison-with-segmentiotopk) on large sketches (k >= 10000).
- A **sliding-window top-K sketch**, also based on HeavyKeeper, as described in ["A Sketch Framework for Approximate Data Stream Processing in Sliding Windows"](https://yangtonghome.github.io/uploads/SlidingSketch_TKDE2022_final.pdf)

```go
import (
	"github.com/keilerkonzept/topk" // plain sketch
	"github.com/keilerkonzept/topk/sliding" // sliding-window sketch
)
```

[Demo application](https://github.com/keilerkonzept/sliding-topk-tui-demo): top K requesting IPs within a sliding time window from a [web server access logs dataset](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)
<p>
    <img src="https://www.keilerkonzept.com/sliding-topk-demo.gif" width="100%" alt="Sliding Top-K Demo Application">
</p>

## Contents

- [Examples](#examples)
    - [Top-K Sketch](#top-k-sketch)
    - [Sliding-window Top-K Sketch](#sliding-window-top-k-sketch)
- [Benchmarks](#benchmarks)
    - [Top-K Sketch](#top-k-sketch)
    - [Sliding-Window Top-K Sketch](#sliding-window-top-k-sketch)
    - [Comparison with segmentio/topk](#comparison-with-segmentiotopk)

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

```
goos: darwin
goarch: arm64
pkg: github.com/keilerkonzept/topk
cpu: Apple M1 Pro
```

The `Add` benchmark performs random increments in the interval [1,10).

| Operation |   K | Depth | Width |        time |  bytes |      allocs |
|-----------|----:|------:|------:|------------:|-------:|------------:|
| `Add`     |  10 |     3 |  1024 | 358.6 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     3 |  8192 | 375.0 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     4 |  1024 | 449.9 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     4 |  8192 | 436.0 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  1024 | 371.5 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  8192 | 387.9 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     4 |  1024 | 452.3 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     4 |  8192 | 471.4 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  1024 | 257.2 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  8192 | 232.3 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     4 |  1024 | 249.1 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     4 |  8192 | 251.2 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  1024 | 264.2 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  8192 | 227.4 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     4 |  1024 | 267.1 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     4 |  8192 | 261.3 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  1024 | 216.0 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  8192 | 215.4 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     4 |  1024 | 220.0 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     4 |  8192 | 269.3 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  1024 | 235.1 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  8192 | 277.1 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     4 |  1024 | 278.7 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     4 |  8192 | 302.2 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  1024 | 129.6 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  8192 | 98.21 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     4 |  1024 | 129.9 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     4 |  8192 | 114.3 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  1024 | 141.2 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  8192 | 140.8 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     4 |  1024 | 131.1 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     4 |  8192 | 109.8 ns/op | 0 B/op | 0 allocs/op |

### Sliding-Window Top-K Sketch

```
goos: darwin
goarch: arm64
pkg: github.com/keilerkonzept/topk/sliding
cpu: Apple M1 Pro
```

The `Add` benchmark performs random increments in the interval [1,10).

| Operation |   K | Depth | Width | Window size | History size |        time |  bytes |      allocs |
|-----------|----:|------:|------:|------------:|-------------:|------------:|-------:|------------:|
| `Add`     |  10 |     3 |  1024 |         100 |           50 | 696.9 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     3 |  1024 |         100 |          100 |  1051 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     3 |  8192 |         100 |           50 | 784.9 ns/op | 0 B/op | 0 allocs/op |
| `Add`     |  10 |     3 |  8192 |         100 |          100 |  1146 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  1024 |         100 |           50 | 712.9 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  1024 |         100 |          100 |  1054 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  8192 |         100 |           50 | 763.3 ns/op | 0 B/op | 0 allocs/op |
| `Add`     | 100 |     3 |  8192 |         100 |          100 |  1139 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  1024 |         100 |           50 | 434.9 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  1024 |         100 |          100 | 560.7 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  8192 |         100 |           50 | 501.1 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    |  10 |     3 |  8192 |         100 |          100 | 728.7 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  1024 |         100 |           50 | 425.6 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  1024 |         100 |          100 | 580.0 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  8192 |         100 |           50 | 497.8 ns/op | 0 B/op | 0 allocs/op |
| `Incr`    | 100 |     3 |  8192 |         100 |          100 | 746.2 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  1024 |         100 |           50 | 228.5 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  1024 |         100 |          100 | 209.3 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  8192 |         100 |           50 | 234.5 ns/op | 0 B/op | 0 allocs/op |
| `Count`   |  10 |     3 |  8192 |         100 |          100 | 230.7 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  1024 |         100 |           50 | 237.5 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  1024 |         100 |          100 | 242.8 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  8192 |         100 |           50 | 246.5 ns/op | 0 B/op | 0 allocs/op |
| `Count`   | 100 |     3 |  8192 |         100 |          100 | 243.4 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  1024 |         100 |           50 | 101.7 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  1024 |         100 |          100 | 104.8 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  8192 |         100 |           50 | 114.0 ns/op | 0 B/op | 0 allocs/op |
| `Query`   |  10 |     3 |  8192 |         100 |          100 | 114.5 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  1024 |         100 |           50 | 135.9 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  1024 |         100 |          100 | 118.5 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  8192 |         100 |           50 | 130.1 ns/op | 0 B/op | 0 allocs/op |
| `Query`   | 100 |     3 |  8192 |         100 |          100 | 131.5 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    |  10 |     3 |  1024 |         100 |           50 |  4191 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    |  10 |     3 |  1024 |         100 |          100 |  7010 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    |  10 |     3 |  8192 |         100 |           50 | 28699 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    |  10 |     3 |  8192 |         100 |          100 | 90979 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    | 100 |     3 |  1024 |         100 |           50 |  6539 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    | 100 |     3 |  1024 |         100 |          100 |  9343 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    | 100 |     3 |  8192 |         100 |           50 | 31349 ns/op | 0 B/op | 0 allocs/op |
| `Tick`    | 100 |     3 |  8192 |         100 |          100 | 87488 ns/op | 0 B/op | 0 allocs/op |

### Comparison with [segmentio/topk](https://github.com/segmentio/topk)

Using [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):
```sh
$ go test -run='^$' -bench=BenchmarkSketchAddForComparison -count=10 | tee new.txt
$ go test -run='^$' -bench=BenchmarkSegmentioTopkSample -count=10 | tee old.txt
$ benchstat -row /K,/Depth,/Width,/Decay -col .name old.txt new.txt
```

```
goos: darwin
goarch: arm64
pkg: github.com/keilerkonzept/topk
cpu: Apple M1 Pro
```

| K      | Depth | Width   | Decay | `segmentio/topk` (sec/op) | this package (sec/op) | diff                   |
|--------|-------|---------|-------|---------------------------|-----------------------|------------------------|
| 10     | 3     | 256     | 0.6   | 641.0n ± 1%               | 373.5n ±  3%          | **-41.73%** (p=0.000 n=10) |
| 10     | 3     | 256     | 0.8   | 602.6n ± 1%               | 387.3n ±  2%          | **-35.73%** (p=0.000 n=10) |
| 10     | 3     | 256     | 0.9   | 550.4n ± 4%               | 431.3n ±  2%          | **-21.63%** (p=0.000 n=10) |
| 100    | 4     | 460     | 0.6   | 763.8n ± 2%               | 427.0n ±  1%          | **-44.09%** (p=0.000 n=10) |
| 100    | 4     | 460     | 0.8   | 720.9n ± 2%               | 459.1n ±  4%          | **-36.30%** (p=0.000 n=10) |
| 100    | 4     | 460     | 0.9   | 660.6n ± 3%               | 539.0n ± 22%          | **-18.41%** (p=0.005 n=10) |
| 1000   | 6     | 6907    | 0.6   | 1107.0n ± 2%              | 555.9n ±  8%          | **-49.79%** (p=0.000 n=10) |
| 1000   | 6     | 6907    | 0.8   | 1040.0n ± 4%              | 613.4n ±  2%          | **-41.02%** (p=0.000 n=10) |
| 1000   | 6     | 6907    | 0.9   | 936.5n ± 1%               | 731.5n ±  2%          | **-21.89%** (p=0.000 n=10) |
| 10000  | 9     | 92103   | 0.6   | 10.693µ ± 2%              | 1.058µ ±  2%          | **-90.11%** (p=0.000 n=10) |
| 10000  | 9     | 92103   | 0.8   | 10.667µ ± 1%              | 1.182µ ±  6%          | **-88.92%** (p=0.000 n=10) |
| 10000  | 9     | 92103   | 0.9   | 10.724µ ± 1%              | 1.288µ ±  2%          | **-87.98%** (p=0.000 n=10) |
| 100000 | 11    | 1151292 | 0.6   | 89.385µ ± 0%              | 1.674µ ±  1%          | **-98.13%** (p=0.000 n=10) |
| 100000 | 11    | 1151292 | 0.8   | 89.349µ ± 1%              | 1.708µ ±  1%          | **-98.09%** (p=0.000 n=10) |
| 100000 | 11    | 1151292 | 0.9   | 89.284µ ± 1%              | 1.705µ ±  1%          | **-98.09%** (p=0.000 n=10) |
