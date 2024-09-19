package topk

type Option func(*Sketch)

// WithDepth sets the depth (number of hash functions) of a sketch.
func WithDepth(depth int) Option { return func(s *Sketch) { s.Depth = depth } }

// WithWidth sets the width (number of counters per hash function) of a sketch.
func WithWidth(width int) Option { return func(s *Sketch) { s.Width = width } }

// WithDecay sets the counter decay probability on collisions.
func WithDecay(decay float32) Option { return func(s *Sketch) { s.Decay = decay } }

// WithDecayLUTSize sets the decay look-up table size.
func WithDecayLUTSize(n int) Option {
	return func(s *Sketch) { s.DecayLUT = make([]float32, n) }
}
