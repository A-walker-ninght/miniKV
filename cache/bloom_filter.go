package cache

import (
	"math"
)

// 使用bitmap存储。
// 0000,0000  -> 一个uint8作为一个计数器，count <= 15
type Counter []byte

type BloomFilter struct {
	bitmap Counter // a Counter -> two counter
	k      uint8
}

func newFilter(m int, fp float64) *BloomFilter {
	bitsPerKey := calBitsPerKey(m, fp)
	return initBloomFilter(m, bitsPerKey)
}

func initBloomFilter(m int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := calHashNum(bitsPerKey)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bf.k = uint8(k)

	nBits := bitsPerKey * m
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)
	filter[nBytes] = uint8(k)
	bf.bitmap = filter
	return bf
}

// 一个key占多少位
// m/n = -lnp/(ln2)^2
func calBitsPerKey(m int, fp float64) int {
	size := -1 * float64(m) * (math.Log(fp)) / math.Log(2)
	locs := math.Ceil(size / float64(m))
	return int(locs)
}

// k = m/n * ln2，k：1~30
func calHashNum(bitsPerKey int) uint32 {
	k := uint32(float64(bitsPerKey) * math.Log(2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

func (bf *BloomFilter) insert(h uint32) bool {
	k := bf.k
	if k > 30 {
		return false
	}
	nBits := uint32((len(bf.bitmap) - 1) * 8)
	delta := h>>17 | h<<15
	for i := uint8(0); i < k; i++ {
		bisPos := h % nBits
		// 0000, 0000
		// 0000, 0000
		// 0000, 0000
		// bitPos/8 是 行
		// bitPost%8 是 列
		bf.bitmap[bisPos/8] |= 1 << (bisPos % 8)
		h += delta
	}
	return true
}

func (bf *BloomFilter) Contain(h uint32) bool {
	// one k
	if len(bf.bitmap) < 2 {
		return false
	}
	k := bf.k
	if k > 30 {
		return false
	}
	nBits := uint32((len(bf.bitmap) - 1) * 8)
	delta := h>>17 | h<<15
	for i := uint8(0); i < k; i++ {
		bitPos := h % nBits
		if bf.bitmap[bitPos/8]&1<<(bitPos%8) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (bf *BloomFilter) FindKey(h uint32) bool {
	if bf == nil {
		return true
	}
	f := bf.Contain(h)
	if !f {
		bf.insert(h)
	}
	return f
}
