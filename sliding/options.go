package sliding

type Option func(*Sketch)

func WithDepth(depth int) Option { return func(s *Sketch) { s.Depth = depth } }

func WithWidth(width int) Option { return func(s *Sketch) { s.Width = width } }

func WithDecay(decay float32) Option { return func(s *Sketch) { s.Decay = decay } }

func WithDecayLUTSize(n int) Option {
	return func(s *Sketch) { s.DecayLUT = make([]float32, n) }
}

func WithBucketHistoryLength(n int) Option {
	return func(s *Sketch) { s.BucketHistoryLength = n }
}
