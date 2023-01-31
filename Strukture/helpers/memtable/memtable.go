package memtable

import (
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
)

// da bi mogli nad oba tipa napisati funkcije pravimo interface
type MemTable interface {
	Put(key string, data *Data)
	Remove(key string)
	Flush()
	Print()
}

//Konstruktor za memtabelu
func NewMemTable(s uint) MemTable{
	config := GetConfig()
	var memTable MemTable
	if config.MemtableStructure == "b_tree"{
		memTable = NewMemTableTree(s)
	} else if config.MemtableStructure == "skiplist"{
		memTable = NewMemTableList(s)
	}
	return memTable
}

func LoadToMemTable(keys []string, data []*Data) MemTable{
	config := GetConfig()
	memtable := NewMemTable(config.MemtableSize)
	for i:=0; i < len(keys); i++{
		memtable.Put(keys[i], data[i])
	}
	return memtable
}

