package memtable

import (
	. "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	. "project/gosaomi/skiplist"
	. "project/gosaomi/sstable"
	. "project/gosaomi/wal"
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

	//Flush
	sstable := NewSSTable(uint32(m.size), GenerateFlushName())
	sstable.Flush(keys, values)
	IncreaseLsmLevel(1)

	//WAL -> kreiramo novi segment(log)
	NewWriteAheadLog("files/wal").NewWALFile().Close()
}

func (m *MemTableList) Put(key string, data *Data) {
	m.slist.Put(key, data)

	if m.slist.GetSize() == m.size {
		m.Flush()
	}
}

func (m *MemTableList) Remove(key string) {
	m.slist.Remove(key)
}