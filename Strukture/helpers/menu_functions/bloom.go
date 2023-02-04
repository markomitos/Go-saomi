package menu_functions

import (
	"fmt"
	"os"
	. "project/gosaomi/bloom"
	. "project/gosaomi/least_reacently_used"
	. "project/gosaomi/memtable"
	. "project/gosaomi/token_bucket"
)

func CreateBloomFilter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	var input string
	blm := new(BloomFilter)

	for true{

		fmt.Print("Unesite kljuc: ")
		fmt.Scanln(&input)
		input = "BloomFilter" + input
		found, _ := GET(input, mem, lru, bucket)
		if found == true {
			fmt.Println("Takav kljuc vec postoji u bazi podataka.")
			return true, input, nil
		}else{
			var expectedNumOfElem uint32
			var falsePositiveRate float64

			//TODO: dodaj validacije
			fmt.Print("ocekivani broj elemenata: ")
			fmt.Scanln(&expectedNumOfElem)
			fmt.Print("Unesite sigurnost tacnosti: ")
			fmt.Scanln(&falsePositiveRate)
			blm = NewBloomFilter(expectedNumOfElem, falsePositiveRate)

			break
		}
	}
	fmt.Println()
	return false, input, blm
}

func GetBloomFilter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	var key string
	blm := new(BloomFilter)

	//unos
	fmt.Print("Unesite kljuc: ")
	fmt.Scanln(&key)
	key = "BloomFilter" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		cmsBytes := data.Value
		blm = MenuByteToBloomFilter(cmsBytes)
		return true, key, blm
	}
	return false, key, blm
}

func BloomFilterAddElement(blm *BloomFilter) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)
	blm.AddToBloom(val)
}

func BloomFilterFindElem(blm *BloomFilter) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da proverite: ")
	fmt.Scanln(&val)

	found := blm.IsInBloom(val)

	if found {
		fmt.Println("Podatak se nalazi u BloomFilteru")
	}
	
	if !found {
		fmt.Println("Podatak se ne nalazi u BloomFilteru")
	}
}

func BloomFilterPUT(key string, blm *BloomFilter, mem MemTable, bucket *TokenBucket) {
	bytesBLM := MenuBloomFilterToByte(blm)
	PUT(key, bytesBLM, mem, bucket)
}

func BloomFilterMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	activeBLM := new(BloomFilter)
	var activeKey string //kljuc Bloom filtera
	var userkey string   //kljuc koji je korisnik uneo i koji se ispisuje korisniku
	userkey = ""
	for true {

		fmt.Println("=======================================")
		fmt.Print("Kljuc aktivnog Bloom filtera: ")
		fmt.Println(userkey)
		fmt.Println()
		fmt.Println("1 - Kreiraj bloom filter")
		fmt.Println("2 - Dobavi bloom filter iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Pronadji element")
		fmt.Println("5 - Upisi bloom filter u bazu podataka")
		fmt.Println("6 - Obrisi bloom filter iz baze podataka")
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
			found, tempKey, tempBLM := CreateBloomFilter(mem, lru, bucket)
			if found {
				fmt.Println("Vec postoji BloomFilter sa datim kljucem")
			} else {
				activeBLM = tempBLM
				activeKey = tempKey
				userkey = activeKey[11:]
			}

		case "2":
			found, key, tempBLM := GetBloomFilter(mem, lru, bucket)
			if found {
				activeBLM = tempBLM
				activeKey = key
				userkey = activeKey[11:]
			} else {
				fmt.Println("Ne postoji BloomFilter sa datim kljucem")
			}
		case "3":

			if len(activeKey) != 0 {
				BloomFilterAddElement(activeBLM)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}

		case "4":
			if len(activeKey) != 0 {
				BloomFilterFindElem(activeBLM)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
		case "5":
			if len(activeKey) != 0 {
				BloomFilterPUT(activeKey, activeBLM, mem, bucket)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
		case "6":
			if len(activeKey) != 0 {
				DELETE(activeKey, mem, lru, bucket)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
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