package lsm

type LSM struct {
	mem   *Memtable
	immem []*Memtable
	level *levelManager
}
