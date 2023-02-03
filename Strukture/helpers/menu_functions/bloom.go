package menu_functions

import (
	"fmt"
	"os"
	. "project/gosaomi/SSTable"
	. "project/gosaomi/bloom"
	. "project/gosaomi/dataType"
	. "project/gosaomi/least_reacently_used"
	. "project/gosaomi/memtable"
	. "project/gosaomi/token_bucket"
	"time"
)

func CreateBloomFileter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (string, *BloomFilter) {
	var input string
	cms := new(BloomFilter)
	for true {

		fmt.Print("Unesite kljuc: ")
		fmt.Scanln(&input)
		input = "BloomFilter" + input
		found, _ := GET(input, mem, lru, bucket)
		if found == true {
			fmt.Println("Takav kljuc vec postoji u bazi podataka. Molimo vas unesite drugi.")
		} else {
			var epsilon float64
			var delta float64

			//TODO: dodaj validacije
			fmt.Print("Unesite preciznost (epsilon): ")
			fmt.Scanln(&epsilon)
			fmt.Print("Unesite sigurnost tacnosti (delta): ")
			fmt.Scanln(&delta)
			cms = NewBloomFilter(epsilon, delta)

			break
		}
	}

	return input, cms
}

// dobavlja cms iz baze podataka
func BloomFileterGET(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	var key string
	cms := new(BloomFilter)

	//unos
	fmt.Print("Unesite kljuc: ")
	fmt.Scanln(&key)
	key = "CountMinSketch" + key

	found, data := GET(key, mem, lru, bucket)
	if found {
		cmsBytes := data.Value
		cms = ByteToBloomFilter(cmsBytes)
		return true, key, cms
	}
	return false, key, cms

}

func BloomFileterAddElement(cms *BloomFilter) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)
	AddToCms(cms, val)
}

func BloomFileterCheckFrequency(cms *BloomFilter) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)

	freq := CheckFrequencyInCms(cms, val)

	fmt.Print("Broj ponavljanja podatka iznosi: ")
	fmt.Println(freq)
}

func BloomFileterPUT(key string, cms *BloomFilter, mem MemTable, bucket *TokenBucket, tombstone bool) {
	data := new(Data)
	bytesCms := CountMinSkechToBytes(cms)
	data.Value = bytesCms
	data.Timestamp = uint64(time.Now().Unix())
	data.Tombstone = tombstone
	PUT(key, data, mem, bucket)
}

func BloomFileterMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	activeCMS := new(BloomFilter)
	var activeKey string
	for true {
		fmt.Println("1 - Kreiraj Bloom Fileter")
		fmt.Println("2 - Dobavi Bloom Fileter iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Proveri broj ponavljanja")
		fmt.Println("5 - Upisi Bloom Fileter u bazu podataka")
		fmt.Println("6 - Obrisi Bloom Fileter iz baze podataka")
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
			activeKey, activeCMS = CreateBloomFileter(mem, lru, bucket)
		case "2":
			found, key, tempCMS := BloomFileterGET(mem, lru, bucket)
			if found {
				activeCMS = tempCMS
				activeKey = key
			} else {
				fmt.Println("Ne postoji CountMinSKetch sa datim kljucem")
			}
		case "3":
			CountMinSketchAddElement(activeCMS)
		case "4":
			CountMinSketchCheckFrequency(activeCMS)
		case "5":
			CountMinSketchPUT(activeKey, activeCMS, mem, bucket, false)
		case "6":
			CountMinSketchPUT(activeKey, activeCMS, mem, bucket, true)
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

}
