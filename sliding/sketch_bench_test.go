package sliding_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/keilerkonzept/topk/sliding"
)

var (
	ks          = []int{10, 100}
	depths      = []int{3}
	widths      = []int{1024, 8192}
	windowSizes = []int{100}
	items       = generateItems(1_000_000)
)

func generateItems(n int) []string {
	items := make([]string, n)
	for i := 0; i < n; i++ {
		items[i] = fmt.Sprintf("item%d", i)
	}
	return items
}

// BenchmarkSketchAdd benchmarks the Add method of Sketch.
func BenchmarkSketchAdd(b *testing.B) {
	for _, k := range ks {
		for _, depth := range depths {
			for _, width := range widths {
				for _, windowSize := range windowSizes {
					for _, historyLen := range []int{windowSize / 2, windowSize} {
						b.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d_WindowSize=%d_HistoryLen=%d", k, depth, width, windowSize, historyLen), func(b *testing.B) {
							sketch := sliding.New(k, windowSize,
								sliding.WithDepth(depth),
								sliding.WithWidth(width),
								sliding.WithBucketHistoryLength(historyLen),
							)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								sketch.Add(items[rand.IntN(len(items))], uint32(rand.IntN(10)))
							}
						})
					}
				}
			}
		}
	}
}

// BenchmarkSketchIncr benchmarks the Incr method of Sketch.
func BenchmarkSketchIncr(b *testing.B) {
	for _, k := range ks {
		for _, depth := range depths {
			for _, width := range widths {
				for _, windowSize := range windowSizes {
					for _, historyLen := range []int{windowSize / 2, windowSize} {
						b.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d_WindowSize=%d_HistoryLen=%d", k, depth, width, windowSize, historyLen), func(b *testing.B) {
							sketch := sliding.New(k, windowSize,
								sliding.WithDepth(depth),
								sliding.WithWidth(width),
								sliding.WithBucketHistoryLength(historyLen),
							)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								sketch.Incr(items[rand.IntN(len(items))])
							}
						})
					}
				}
			}
		}
	}
}

// BenchmarkSketchCount benchmarks the Count method of Sketch.
func BenchmarkSketchCount(b *testing.B) {
	for _, k := range ks {
		for _, depth := range depths {
			for _, width := range widths {
				for _, windowSize := range windowSizes {
					for _, historyLen := range []int{windowSize / 2, windowSize} {
						b.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d_WindowSize=%d_HistoryLen=%d", k, depth, width, windowSize, historyLen), func(b *testing.B) {
							sketch := sliding.New(k, windowSize,
								sliding.WithDepth(depth),
								sliding.WithWidth(width),
								sliding.WithBucketHistoryLength(historyLen),
							)
							for _, item := range items {
								sketch.Add(item, uint32(rand.IntN(10)))
							}

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								sketch.Count(items[rand.IntN(len(items))])
							}
						})
					}
				}
			}
		}
	}
}

// BenchmarkSketchQuery benchmarks the Query method of Sketch.
func BenchmarkSketchQuery(b *testing.B) {
	for _, k := range ks {
		for _, depth := range depths {
			for _, width := range widths {
				for _, windowSize := range windowSizes {
					for _, historyLen := range []int{windowSize / 2, windowSize} {
						b.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d_WindowSize=%d_HistoryLen=%d", k, depth, width, windowSize, historyLen), func(b *testing.B) {
							sketch := sliding.New(k, windowSize,
								sliding.WithDepth(depth),
								sliding.WithWidth(width),
								sliding.WithBucketHistoryLength(historyLen),
							)
							for _, item := range items {
								sketch.Add(item, uint32(rand.IntN(10)))
							}

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								sketch.Query(items[rand.IntN(len(items))])
							}
						})
					}
				}
			}
		}
	}
}

// BenchmarkSketchTick benchmarks the Tick method of Sketch.
func BenchmarkSketchTick(b *testing.B) {
	for _, k := range ks {
		for _, depth := range depths {
			for _, width := range widths {
				for _, windowSize := range windowSizes {
					for _, historyLen := range []int{windowSize / 2, windowSize} {
						b.Run(fmt.Sprintf("K=%d_Depth=%d_Width=%d_WindowSize=%d_HistoryLen=%d", k, depth, width, windowSize, historyLen), func(b *testing.B) {
							sketch := sliding.New(k, windowSize,
								sliding.WithDepth(depth),
								sliding.WithWidth(width),
								sliding.WithBucketHistoryLength(historyLen),
							)

							for _, item := range items {
								sketch.Add(item, uint32(rand.IntN(10)))
							}

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								sketch.Tick()
							}
						})
					}
				}
			}
		}
	}
}
