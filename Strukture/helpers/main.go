package main

import (
	"fmt"
	"os"

	// . "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	. "project/gosaomi/memtable"
	. "project/gosaomi/token_bucket"
	. "project/gosaomi/wal"
	// . "project/gosaomi/menu_functions"
	// . "project/gosaomi/sstable"
)

func menu() {
	fmt.Println("1 - PUT")
	fmt.Println("2 - GET")
	fmt.Println("3 - DELETE")
	fmt.Println("4 - LIST")
	fmt.Println("5 - RANGE SCAN")
	fmt.Println("X - Izlaz iz programa")
	fmt.Println("=======================================")
	fmt.Print("Izaberite opciju: ")

	var input string
	n, err := fmt.Scanln(&input)

	if err != nil {
		fmt.Println("Greska prilikom unosa: ", err)
	} else if n == 0 {
		fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		return
	}

	switch input {
	case "1":
		fmt.Println("1")
	case "2":
		fmt.Println("2")
	case "3":
		fmt.Println("3")
	case "4":
		fmt.Println("4")
	case "5":
		fmt.Println("5")
	case "x":
		fmt.Println("Vidimo se sledeci put!")
		os.Exit(0)
	case "X":
		fmt.Println("Vidimo se sledeci put!")
		os.Exit(0)
	default:
		fmt.Println("Neispravan unos. Molimo vas probajte opet.")
	}
}

func main() {
	//Ucitavamo strukturu fajlova
	InitializeLsm()
	
	//Ucitavamo CACHE (LRU)

	//Na pocetku ucitavamo iz WAL-a u memtabelu
	wal := NewWriteAheadLog("files/wal")
	memtable := LoadToMemTable(wal.InitiateMemTable())
	fmt.Println(memtable)

	//Ogranicenje brzine pristupa
	bucket := NewTokenBucket()
	fmt.Println(bucket)

	

	// for i:=0; i < 500; i++{
	// 	data := new(Data)
	// 	data.Value = []byte("majmun")
	// 	data.Timestamp = uint64(time.Now().Unix())
	// 	data.Tombstone = false
	// 	key := strconv.Itoa(i)
		
	// 	if !PUT(key,data,memtable,bucket){
	// 		fmt.Println("MAJMUNE")
	// 	} else {
	// 		fmt.Println("PROSLO")
	// 	}
	// 	time.Sleep(time.Millisecond * 100)
	// }

	// NewWriteAheadLog("files/wal").ReadAllLogs()
	
	RunCompact()

	fmt.Println("====== DOBRODOSLI U KEY-VALUE ENGINE ======")
	for true {
		menu()
	}

	// keys := make([]string, 0)
	// values := make([]*Data, 0)
	// bTree.InorderTraverse(bTree.Root, &keys, &values)
	// for i := 0; i < len(keys); i++ {
	// 	fmt.Println("Key: ", keys[i], "Value: ", string(values[i].Value))
	// }

	//SSTABELA
	// sstable := NewSSTable(uint32(bTree.Size), "sstable-1")
	// sstable.Flush(keys, values)
	// sstable.ReadData()
	// sstable.ReadIndex()
	// sstable.ReadSummary()
	// sstable.ReadBloom()

	// found1, data1 := sstable.Find("b")
	// if !found1 {
	// 	fmt.Println("Nije pronadjen b")
	// } else {
	// 	fmt.Println(data1)
	// }

	// found2, data2 := sstable.Find("x")
	// if !found2 {
	// 	fmt.Println("Nije pronadjen x")
	// } else {
	// 	fmt.Println(data2)
	// }

	// found3, data3 := sstable.Find("aaaaaaa")
	// if !found3 {
	// 	fmt.Println("Nije pronadjen aaaaaaa")
	// } else {
	// 	fmt.Println(data3)
	// }

	
}
