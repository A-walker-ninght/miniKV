package codec

import "encoding/binary"

type Entry struct {
	Key     string
	Value   []byte
	Deleted bool // 该数据是否已经被删除
	Version int64
}

func NewEntry(key string, value []byte) Entry {
	e := Entry{
		Key:   key,
		Value: value,
	}
	return e
}

func ValueToBytes(value interface{}) []byte {
	if value == nil {
		return []byte{}
	}
	switch k := value.(type) {
	case string:
		return []byte(k)
	case []byte:
		return k
	case int16:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(k))
		return b
	case int32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(k))
		return b
	case int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(k))
		return b
	default:
		panic("Value type not supported")
	}
}
