package memtable

import (
	. "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	. "project/gosaomi/skiplist"
	. "project/gosaomi/sstable"
)

type MemTableList struct {
	size  uint
	slist *SkipList
}

// konstuktor za skiplistu
func NewMemTableList(s uint) *MemTableList {
	m := new(MemTableList)
	m.slist = NewSkipList(s)
	m.size = s
	return m
}

func (m *MemTableList) Print() {
	m.slist.Print()
}

func (m *MemTableList) Flush() {
	keys := make([]string, 0)
	values := make([]*Data, 0)
	//dobavi sve sortirane podatke
	m.slist.GetAllNodes(&keys, &values)

	//praznjenje skipliste
	newSkiplist := NewSkipList(m.size)
	m.slist = newSkiplist
	IncreaseLsmLevel(1)

	//Flush
	sstable := NewSSTable(uint32(m.size), GenerateFlushName())
	sstable.Flush(keys, values)
	IncreaseLsmLevel(1)
}

func (m *MemTableList) Put(key string, value []byte, tombstone ...bool) {
	if len(tombstone) > 0 {
		m.slist.Put(key, value, tombstone[0])
	} else {
		m.slist.Put(key, value)
	}

	if m.slist.GetSize() == m.size {
		m.Flush()
	}
}

func (m *MemTableList) Remove(key string) {
	m.slist.Remove(key)
}