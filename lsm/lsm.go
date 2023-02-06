package lsm

type LSM struct {
	mem   *memtable
	immem []*memtable
	level *levelManager
	
}
