package menu_functions

import (
	"fmt"
	. "project/keyvalue/structures/bloom"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/token_bucket"
)

func CreateBloomFilter(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *BloomFilter) {
	var input string //kljuc
	blm := new(BloomFilter)
	var expectedNumOfElem uint32
	var falsePositiveRate float64


		input = GetKeyInput()
		if input == "*" {
			return true, input, nil
		}
		input = "BloomFilter" + input
		found, data := GET(input, mem, lru, bucket)
		if found == true {
			var choice string

			for true {

				fmt.Println("Takav kljuc vec postoji u bazi podataka. Da li zelite da:")
				fmt.Println("1. Dobavite ovaj BloomFilter iz baze podataka")
				fmt.Println("2. Napravite novi BloomFilter pod ovim kljucem")
				fmt.Print("Unesite 1 ili 2: ")
				n, err := fmt.Scanln(&choice)

				if err != nil {
					fmt.Println("Greska prilikom unosa: ", err)
				} else if n == 0 {
					fmt.Println("Prazan unos.  Molimo vas probajte opet.")
				//ukoliko nema greske:
				} else {
					if choice == "1" {
						blm = MenuByteToBloomFilter(data.Value)
						return false, input, blm
	
					}else if choice == "2"{
	
						fmt.Print("ocekivani broj elemenata: ")
						fmt.Scanln(&expectedNumOfElem)
						fmt.Print("Unesite sigurnost tacnosti: ")
						fmt.Scanln(&falsePositiveRate)
	
						blm = NewBloomFilter(expectedNumOfElem, falsePositiveRate)
						return false, input, blm
					} else{
						fmt.Println("Molimo vas unesite 1 ili 2")
					}
				}

				
			}
			
			return true, input, nil
		}else{

			//TODO: dodaj validacije
			fmt.Print("ocekivani broj elemenata: ")
			fmt.Scanln(&expectedNumOfElem)
			fmt.Print("Unesite sigurnost tacnosti: ")
			fmt.Scanln(&falsePositiveRate)
			blm = NewBloomFilter(expectedNumOfElem, falsePositiveRate)

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
		fmt.Println("X - Povratak na glavni meni")
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
			if tempKey != "*" {
				if found {
					fmt.Println("Vec postoji BloomFilter sa datim kljucem")
				} else {
					activeBLM = tempBLM
					activeKey = tempKey
					userkey = activeKey[11:]
					fmt.Println("Uspesno kreiranje")
				}
			}
			
		case "2":
			found, key, tempBLM := GetBloomFilter(mem, lru, bucket)
			if found {
				activeBLM = tempBLM
				activeKey = key
				userkey = activeKey[11:]
				fmt.Println("Uspesno dobavljanje")
			} else {
				fmt.Println("Ne postoji BloomFilter sa datim kljucem")
			}
		case "3":

			if len(activeKey) != 0 {
				BloomFilterAddElement(activeBLM)
				fmt.Println("Uspesno dodavanje")
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
				fmt.Println("Uspesan upis")
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
			}
		case "6":
			if len(activeKey) != 0 {
				DELETE(activeKey, mem, lru, bucket)
			} else {
				fmt.Println("Nije izabran aktivni BloomFilter")
				fmt.Println("Uspesno brisanje")
			}
		case "x":
			return
		case "X":
			return
		default:
			fmt.Println("Neispravan unos. Molimo vas probajte opet.")
		}
	}
}