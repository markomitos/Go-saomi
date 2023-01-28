package memtable

import (
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

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
	m.btree = NewBTree(3)
	return m

}

// TO DO implementirati flush za obe strukture - da isprazni memtable i stavi ga u SSTable na disku
func (m MemTableTree) Flush() {
	//dobavi sve sortirane podatke
	nodelist := m.btree.GetAllNodes()

	//TODO: posalji podatke SStabeli
	fmt.Println(nodelist) //stoji print da ne bi prijavljivao gresku

	//praznjenje skipliste
	newSkiplist := newSkipList(m.size)
	m.slist = newSkiplist
}

func (m *MemTableList) Flush() {

	//dobavi sve sortirane podatke
	nodelist := m.slist.getAllNodes()

	//TODO: posalji podatke SStabeli
	fmt.Println(nodelist) //stoji print da ne bi prijavljivao gresku

	//praznjenje skipliste
	newSkiplist := NewSkipList(m.size)
	m.slist = newSkiplist
}

func (m MemTableTree) Put(key uint, value []byte, timestamp []byte) {
	m.btree.InsertElem(key, value, timestamp)
	if m.btree.size == m.size {
		m.Flush()
	}
}

func (m *MemTableList) Put(key string, value []byte, timestamp []byte) {
	m.slist.Put(key, value, timestamp)
	if m.slist.size == m.size {
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

	mem_table := NewMemTableList(config.MemtableSize)
	mem_table.Put("1", []byte("majmun"), []byte("vreme"))
	mem_table.Put("i", []byte("majmun"), []byte("vreme"))
	mem_table.Put("c", []byte("majmun"), []byte("vreme"))
	mem_table.Put("e", []byte("majmun"), []byte("vreme"))
	mem_table.Put("d", []byte("majmun"), []byte("vreme"))
	mem_table.Put("f", []byte("majmun"), []byte("vreme"))
	mem_table.Put("g", []byte("majmun"), []byte("vreme"))
	mem_table.Put("s", []byte("majmun"), []byte("vreme"))
	mem_table.Put("q", []byte("majmun"), []byte("vreme"))
	mem_table.Put("r", []byte("majmun"), []byte("vreme"))
	mem_table.Put("t", []byte("majmun"), []byte("vreme"))
	mem_table.slist.print()

	//ne treba za proj marshal jer necemo zapisivati samo citati
	marshalled, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(marshalled))
}
