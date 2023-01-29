package main

import (
	"fmt"
	"io/ioutil"
	"log"

	. "project/gosaomi/b_tree"
	. "project/gosaomi/dataType"
	. "project/gosaomi/skiplist"

	"gopkg.in/yaml.v2"
)

const numberOfChildren = 3

type Config struct {
	//stringovi posle atributa su tu da bi Unmarshal znao gde sta da namapira
	WalSize           int    `yaml:"wal_size"`
	MemtableSize      int    `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
}

type MemTableTree struct {
	size  uint
	btree *BTree
}

type MemTableList struct {
	size  int
	slist *SkipList
}

// da bi mogli nad oba tipa napisati funkcije pravimo interface
type MemTable interface {
	Put(key []byte, value []byte, timestamp []byte)
	Remove(key []byte, velue []byte, timestamp []byte)
	Flush()
}

// konstuktor za skiplistu
func NewMemTableList(s int) *MemTableList {
	m := new(MemTableList)
	m.slist = NewSkipList(s)
	m.size = s
	return m

}

// konstruktor za b stablo
func NewMemTableTree(s int) *MemTableTree {
	m := new(MemTableTree)
	m.btree = NewBTree(numberOfChildren)
	return m

}

// TO DO implementirati flush za obe strukture - da isprazni memtable i stavi ga u SSTable na disku
func (m *MemTableTree) Flush() {
	//dobavi sve sortirane podatke

	keys := make([]string, 0)
	values := make([]*Data, 0)
	m.btree.InorderTraverse(m.btree.Root, &keys, &values)

	//TODO: posalji podatke SStabeli

	//praznjenje b_stabla
	newBTree := NewBTree(numberOfChildren)
	m.btree = newBTree
}

func (m *MemTableList) Flush() {

	keys := make([]string, 0)
	values := make([]*Data, 0)
	//dobavi sve sortirane podatke
	m.slist.GetAllNodes(&keys, &values)

	//TODO: posalji podatke SStabeli
	fmt.Println(keys)
	for i := 0; i < 10; i++ {
		fmt.Println(values[i])
	}
	//praznjenje skipliste
	newSkiplist := NewSkipList(m.size)
	m.slist = newSkiplist
}

func (m MemTableTree) Put(key string, value []byte, tombstone ...bool) {
	if len(tombstone) > 0 {
		m.btree.InsertElem(key, value, tombstone[0])
	} else {
		m.btree.InsertElem(key, value)
	}

	if m.btree.Size == m.size {
		m.Flush()
	}
}

func (m *MemTableList) Put(key string, value []byte, tombstone ...bool) {
	m.slist.Put(key, value)
	if m.slist.GetSize() == m.size {
		m.Flush()
	}
}

func (m *MemTableList) Remove(key string) {
	m.slist.Remove(key)
}

func (m *MemTableTree) Remove(key string) {
	m.btree.Remove(key)
}

func main() {
	var config Config
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	//upisuje sve iz fileu u osobine configu
	yaml.Unmarshal(configData, &config)
	fmt.Println(config)

	// u zavisnosti sta pise u configu pravimo il btree il skiplistu -- NE MOZE OVAKO
	// if config.MemtableStructure == "btree" {
	// 	mem_table := newMemTableTree(config.MemtableSize)
	// } else {
	// 	mem_table := newMemTableList(config.MemtableSize)
	// }

	mem_table := NewMemTableTree(config.MemtableSize)
	mem_table.Put("1", []byte("majmun"))
	mem_table.Put("i", []byte("majmun"))
	mem_table.Put("c", []byte("majmun"))
	mem_table.Put("e", []byte("majmun"))
	mem_table.Put("d", []byte("majmun"))
	mem_table.Put("f", []byte("alobre213"))
	mem_table.Put("g", []byte("majmun"))
	mem_table.Put("s", []byte("majmun"))
	mem_table.Put("q", []byte("majmun"))
	mem_table.Put("r", []byte("majmun"))
	mem_table.Put("t", []byte("majmun"))
	mem_table.btree.PrintBTree()

	//ne treba za proj marshal jer necemo zapisivati samo citati
	marshalled, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(marshalled))
}
