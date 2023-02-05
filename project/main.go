package main

import (
	"fmt"
	"math/rand"
	"os"

	// "strconv"

	"time"

	// "time"

	// . "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/lsm"
	. "project/keyvalue/structures/memtable"

	. "project/keyvalue/menu_functions"
	. "project/keyvalue/structures/token_bucket"
	. "project/keyvalue/structures/wal"
	// . "project/keyvalue/structures/sstable"
)

//RANDOM STRING GENERATOR
const charset = "abcdefghijklmnopqrstuvwxyz"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandomString(length int) string {
	return StringWithCharset(length, charset)
}
//--------------------------


func menu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	fmt.Println("=======================================")
	fmt.Println("1 - PUT")
	fmt.Println("2 - GET")
	fmt.Println("3 - DELETE")
	fmt.Println("4 - LIST")
	fmt.Println("5 - RANGE SCAN")
	fmt.Println("6 - KOMPAKCIJA")
	fmt.Println("7 - CountMinSketch menu")
	fmt.Println("8 - BloomFilter menu")
	fmt.Println("9 - HyperLogLog menu")
	fmt.Println("10 - SimHash menu")
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
		key, val := GetUserInput()
		if key != "*"{
			PUT(key, val, mem, bucket)
		}
	case "2":
		key:= GetKeyInput()
		if key != "*"{
			found, data := GET(key, mem, lru, bucket)
			if found {
				data.Print()
			} else {
				fmt.Println("Kljuc se ne nalazi u bazi podataka")
			}
		}
		
	case "3":
		key:= GetKeyInput()
		if key != "*"{
			DELETE(key, mem, lru, bucket)
		}
	case "4":
		InitiateListScan(mem)
	case "5":
		InitiateRangeScan(mem)
	case "6":
		RunCompact()
	case "7":
		CountMinSKetchMenu(mem, lru, bucket)
	case "8":
		BloomFilterMenu(mem, lru, bucket)
	case "9":
		HyperLogLogMenu(mem, lru, bucket)
	case "10":
		SimHashMenu(mem, lru, bucket)
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
	//inicijalizujemo strukturu fajlova
	InitializeLsm()
	
	//Ucitavamo CACHE (LRU)
	lru := ReadLru()
	fmt.Println(lru)

	//Na pocetku ucitavamo iz WAL-a u memtabelu
	wal := NewWriteAheadLog("files/wal")
	memtable := LoadToMemTable(wal.InitiateMemTable())
	fmt.Println(memtable)

	//Ogranicenje brzine pristupa
	bucket := NewTokenBucket()
	fmt.Println(bucket)



	// for i:=1023; i > 0; i--{
	// 	value := []byte(RandomString(5))
	// 	key := strconv.FormatInt(int64(i),10)
		
	// 	if !PUT(key,value,memtable,bucket){
	// 		fmt.Println("MAJMUNE")
	// 	} else {
	// 		fmt.Println("PROSLO")
	// 	}
	// 	time.Sleep(time.Millisecond * 100)
	// }
	
	// start := time.Now()
	// found, data := GET("100", memtable, lru, bucket)
	// fmt.Printf("main, execution time %s\n", time.Since(start))
	// if found {
	// 	fmt.Println("100")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 100")
	// }

	// DELETE("560", memtable, lru, bucket)
	// value := []byte("aligator")
	// PUT("581",value,memtable,bucket)


	// RunCompact()

	start := time.Now()
	found, keys, dataArr := LIST_SCAN("1", 15 , 1, memtable)
	fmt.Printf("main, execution time %s\n", time.Since(start))

	if !found {
		fmt.Println("Nema pronadjenih rezultata")
	} else {
		for i:=0; i < len(keys); i++{
			fmt.Println(keys[i])
			dataArr[i].Print()
			fmt.Println("********************************")
		}
	}

	// ReadLsm().Print()

	fmt.Println("====== DOBRODOSLI U KEY-VALUE ENGINE ======")
	fmt.Println("Ukoliko zelite da izadjete iz bilo koje funkcije, UNESITE '*'")
	for true {
		menu(memtable, lru, bucket)
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

