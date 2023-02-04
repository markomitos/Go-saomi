package main

import (
	"fmt"
	"math/rand"
	"os"

	"strconv"

	"time"

	// "time"

	. "project/gosaomi/dataType"
	. "project/gosaomi/least_reacently_used"
	"project/gosaomi/lsm"
	. "project/gosaomi/lsm"
	. "project/gosaomi/memtable"

	. "project/gosaomi/menu_functions"
	. "project/gosaomi/token_bucket"
	. "project/gosaomi/wal"
	// . "project/gosaomi/sstable"
)

//RANDOM STRING GENERATOR
const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

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
		PUT(key, val, mem, bucket)
	case "2":
		key:= GetKeyInput()
		GET(key, mem, lru, bucket)
	case "3":
		key:= GetKeyInput()
		DELETE(key, mem, lru, bucket)
	case "4":
		fmt.Println("4")
	case "5":
		fmt.Println("5")
	case "6":
		fmt.Println("5")
	case "7":
		CountMinSKetchMenu(mem, lru, bucket)
	case "8":
		BloomFilterMenu(mem, lru, bucket)
	case "9":
		fmt.Println("5")
	case "10":
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


	for i:=586; i > 0; i--{
		data := new(Data)
		data.Value = []byte("majmun")
		data.Timestamp = uint64(time.Now().Unix())
		data.Tombstone = false
		key := strconv.FormatInt(int64(i),10)
		
		if !PUT(key,data,memtable,bucket){
			fmt.Println("MAJMUNE")
		} else {
			fmt.Println("PROSLO")
		}
		time.Sleep(time.Millisecond * 100)
	}
	
	// start := time.Now()
	// found, data := GET("2", memtable, lru, bucket)
	// fmt.Printf("main, execution time %s\n", time.Since(start))
	// if found {
	// 	fmt.Println("2")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 2")
	// }

	// start = time.Now()
	// found, data = GET("2", memtable, lru, bucket)
	// fmt.Printf("main, execution time %s\n", time.Since(start))
	// if found {
	// 	fmt.Println("2")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 2")
	// }

	// DELETE("2", memtable, lru, bucket)

	// start = time.Now()
	// found, data = GET("2", memtable, lru, bucket)
	// fmt.Printf("main, execution time %s\n", time.Since(start))
	// if found {
	// 	fmt.Println("2")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 2")
	// }


	// found, data = GET("20", memtable, lru, bucket)
	// if found {
	// 	fmt.Println("20")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 20")
	// }

	// found, data = GET("3", memtable, lru, bucket)
	// if found {
	// 	fmt.Println("3")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem 3")
	// }

	// found, data = GET("abc", memtable, lru, bucket)
	// if found {
	// 	fmt.Println("abc")
	// 	data.Print()
	// } else {
	// 	fmt.Println("Ne postoji podatak sa kljucem abc")
	// }

	// DELETE("560", memtable, lru, bucket)

	data := new(Data)
	data.Value = []byte("aligator")
	data.Timestamp = uint64(time.Now().Unix())
	data.Tombstone = false
	PUT("581",data,memtable,bucket)

	// start := time.Now()
	// found, keys, dataArr := RANGE_SCAN("1", "99999", 7 , 1, memtable)
	// fmt.Printf("main, execution time %s\n", time.Since(start))

	// if !found {
	// 	fmt.Println("Nema pronadjenih rezultata")
	// } else {
	// 	for i:=0; i < len(keys); i++{
	// 		fmt.Println(keys[i])
	// 		dataArr[i].Print()
	// 		fmt.Println("********************************")
	// 	}
	// }

	lsm.ReadLsm().Print()

	fmt.Println("====== DOBRODOSLI U KEY-VALUE ENGINE ======")
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

