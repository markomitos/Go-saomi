package memtable

import (
	. "project/gosaomi/b_tree"
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	. "project/gosaomi/sstable"
	. "project/gosaomi/wal"
)

type MemTableTree struct {
	size  uint
	btree *BTree
}

// konstruktor za b stablo
func NewMemTableTree(s uint) *MemTableTree {
	config := GetConfig()
	m := new(MemTableTree)
	m.size = s
	m.btree = NewBTree(config.BTreeNumOfChildren)
	return m

}

func (m *MemTableTree) Print() {
	m.btree.PrintBTree()
}

func (m *MemTableTree) Flush() {
	config := GetConfig()

	//dobavi sve sortirane podatke
	keys := make([]string, 0)
	values := make([]*Data, 0)
	m.btree.InorderTraverse(m.btree.Root, &keys, &values)

	//praznjenje b_stabla i rotacija
	newBTree := NewBTree(config.BTreeNumOfChildren)
	m.btree = newBTree

	//Flush
	sstable := NewSSTable(uint32(m.size), GenerateFlushName())
	sstable.Flush(keys, values)
	IncreaseLsmLevel(1)

	//WAL -> kreiramo novi segment(log)
	NewWriteAheadLog("files/wal").NewWALFile().Close()
}

func (m *MemTableTree) Put(key string, data *Data) {
	m.btree.Put(key, data)

	if m.btree.Size == m.size {
		m.Flush()
	}
}

func (m *MemTableTree) Remove(key string) {
	m.btree.Remove(key)
}