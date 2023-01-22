package cache

type CMCounter []byte // two counter

// 0000,0000
// 0000,0000
// 0000,0000
type CMSketch struct {
	counters []CMCounter
}
