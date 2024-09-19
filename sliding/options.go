package sliding

type Option func(*Sketch)

// WithDepth sets the depth (number of hash functions) of a sketch.
func WithDepth(depth int) Option { return func(s *Sketch) { s.Depth = depth } }

// WithWidth sets the width (number of buckets per hash function) of a sketch.
func WithWidth(width int) Option { return func(s *Sketch) { s.Width = width } }

// WithDecay sets the counter decay probability on collisions.
func WithDecay(decay float32) Option { return func(s *Sketch) { s.Decay = decay } }

// WithDecayLUTSize sets the decay look-up table size.
func WithDecayLUTSize(n int) Option {
	return func(s *Sketch) { s.DecayLUT = make([]float32, n) }
}

// WithBucketHistoryLength sets the number of old counters to keep per bucket.
//
// This parameter primarily affects the precision of the [Sketch.Tick] method:
//   - The sliding window is accurate (modulo counter error) if there as many counters as there are ticks in the window.
//   - If there are fewer old counters, the counts of several ticks are collected in one counter, resulting in imprecise aging.
func WithBucketHistoryLength(n int) Option {
	return func(s *Sketch) { s.BucketHistoryLength = n }
}
