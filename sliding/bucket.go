package sliding

// Bucket is a single counter together with its history and the corresponding item's fingerprint.
type Bucket struct {
	Fingerprint uint32

	// Counts is a circular buffer (with its first entry at .First)
	Counts []uint32
	First  uint32
	// CountsSum is the current sum of Counts
	CountsSum uint32
}

func (me *Bucket) tick() {
	if me.CountsSum == 0 {
		return
	}

	last := me.First
	if last == 0 {
		last = uint32(len(me.Counts) - 1)
	} else {
		last = uint32(last - 1)
	}
	me.CountsSum -= me.Counts[last]
	me.Counts[last] = 0
	me.First = last
}

func (me *Bucket) findNonzeroMinimumCount() int {
	countsMinIdx := uint32(0)
	first := true
	var countsMin uint32
	i := me.First
	for range me.Counts {
		if i == uint32(len(me.Counts)) {
			i = 0
		}
		c := me.Counts[i]
		if c == 0 {
			i++
			continue
		}
		if first || c < countsMin {
			countsMin = c
			countsMinIdx = i
			first = false
		}
		i++
	}
	return int(countsMinIdx)
}
