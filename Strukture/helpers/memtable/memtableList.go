package memtable

import (
	. "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	. "project/gosaomi/scan"
	. "project/gosaomi/skiplist"
	. "project/gosaomi/sstable"
	. "project/gosaomi/wal"
	"time"
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

func (m *MemTableList) Find(key string) (bool, *Data){
	node, found  := m.slist.Find(key)
	if !found {
		return false, nil
	}
	return true, node.Data
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
	//Ukoliko nije nasao trazeni kljuc u Memtable
	//Dodaje ga kao novi element sa tombstone=true
	if !m.slist.Remove(key){
		data:= new(Data)
		data.Timestamp = uint64(time.Now().Unix())
		data.Tombstone = true
		data.Value = make([]byte, 0)
		m.Put(key,data)
	}
}

func (m *MemTableList) RangeScan(minKey string, maxKey string, scan *Scan){
	m.slist.RangeScan(minKey, maxKey, scan)
}