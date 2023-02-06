package Iterator

import (
	"github.com/A-walker-ninght/miniKV/codec"
)

type Interator interface {
	Next()
	Valid() bool
	First()
	Entry() *codec.Entry
	Seek(key string)
}
