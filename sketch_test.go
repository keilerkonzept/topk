package topk_test

import (
	"fmt"
	"testing"

	"github.com/keilerkonzept/topk"
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
