package memtable

import (
	"fmt"
	"log"
	. "project/gosaomi/config"

	"gopkg.in/yaml.v2"
)

// da bi mogli nad oba tipa napisati funkcije pravimo interface
type MemTable interface {
	Put(key string, value []byte, tombstone ...bool)
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


func main() {
	config := GetConfig()

	// u zavisnosti sta pise u configu pravimo il btree il skiplistu -- NE MOZE OVAKO
	var mem_table MemTable
	if config.MemtableStructure == "btree" {
		mem_table = NewMemTableTree(config.MemtableSize)
	} else {
		mem_table = NewMemTableList(config.MemtableSize)
	}

	mem_table.Put("1", []byte("majmun"))
	mem_table.Put("i", []byte("majmun"))
	mem_table.Put("c", []byte("majmun"))
	mem_table.Put("e", []byte("majmun"))
	mem_table.Put("d", []byte("majmun"))
	mem_table.Put("f", []byte("alobre213"))
	mem_table.Remove("f")
	mem_table.Put("g", []byte("majmun"))
	mem_table.Put("s", []byte("majmun"))
	mem_table.Put("q", []byte("majmun"))
	mem_table.Put("r", []byte("majmun"))
	// mem_table.Put("t", []byte("majmun"))
	mem_table.Print()

	//ne treba za proj marshal jer necemo zapisivati samo citati
	marshalled, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(marshalled))
}
