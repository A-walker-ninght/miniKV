package codec

import "time"

// 存储的数据
type EntryStruct struct {
}

// 上层的数据
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiredAt uint64
	Version   uint32
}

func (e *Entry) Keys() uint32 {
	return Murmurhash2(e.Key)
}

func (e *Entry) WithTTL(dur time.Duration) {
	e.ExpiredAt = uint64(time.Now().Add(dur).UnixNano())
}
