package codec

type Status int

const (
	Found Status = iota
	NotFound
	Deleted
)

type Entry struct {
	Key     string
	Value   []byte
	Deleted bool // 该数据是否已经被删除
}

func NewEntry(key string, value []byte) Entry {
	e := Entry{
		Key:   key,
		Value: value,
	}
	return e
}
