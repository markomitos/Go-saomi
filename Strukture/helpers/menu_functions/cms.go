package menu_functions

import (
	"fmt"
	. "project/gosaomi/cms"
	. "project/gosaomi/least_reacently_used"
	. "project/gosaomi/memtable"
	. "project/gosaomi/token_bucket"
)

func CreateCountMinSketch(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *CountMinSketch) {
	var input string
	var epsilon float64
	var delta float64
	cms := new(CountMinSketch)
	for true{

		fmt.Print("Unesite kljuc: ")
		fmt.Scanln(&input)
		input = "CountMinSketch" + input
		found, data := GET(input, mem, lru, bucket)
		if found == true {
			var choice string

			for true {

				fmt.Println("Takav kljuc vec postoji u bazi podataka. Da li zelite da:")
				fmt.Println("1. Dobavite ovaj CountMinSketch iz baze podataka")
				fmt.Println("2. Napravite novi CountMinSketch pod ovim kljucem")
				fmt.Print("Unesite 1 ili 2: ")
				fmt.Scanln(&choice)

				if choice == "1" {
					cms = BytesToCountMinSketch(data.Value)
					return false, input, cms

				}else if choice == "2"{

					fmt.Print("Unesite preciznost (epsilon): ")
					fmt.Scanln(&epsilon)
					fmt.Print("Unesite sigurnost tacnosti (delta): ")
					fmt.Scanln(&delta)

					cms = NewCountMinSketch(epsilon, delta)
					return false, input, cms
				} else{
					fmt.Println("Molimo vas unesite 1 ili 2")
				}
			}
			return true, input, nil
		}else {

			//TODO: dodaj validacije
			fmt.Print("Unesite preciznost (epsilon): ")
			fmt.Scanln(&epsilon)
			fmt.Print("Unesite sigurnost tacnosti (delta): ")
			fmt.Scanln(&delta)
			cms = NewCountMinSketch(epsilon, delta)

			break
		}
	}

	return false, input, cms
}
//dobavlja cms iz baze podataka
func CountMinSketchGET(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *CountMinSketch) {
	var key string
	cms := new(CountMinSketch)

	//unos
	fmt.Print("Unesite kljuc: ")
	fmt.Scanln(&key)
	key = "CountMinSketch" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		cmsBytes := data.Value
		cms = BytesToCountMinSketch(cmsBytes)
		return true, key, cms
	}
	return false, key, cms

}

func CountMinSketchAddElement(cms *CountMinSketch) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)
	AddToCms(cms, val)
}

func CountMinSketchCheckFrequency(cms *CountMinSketch) {
	var val []byte

	//unos
	fmt.Print("Unesite podatak koji zelite da proverite: ")
	fmt.Scanln(&val)

	freq := CheckFrequencyInCms(cms, val)

	fmt.Print("Broj ponavljanja podatka iznosi: ")
	fmt.Println(freq)
}

func CountMinSketchPUT(key string, cms *CountMinSketch, mem MemTable, bucket *TokenBucket) {
	bytesCms := CountMinSkechToBytes(cms)
	PUT(key, bytesCms, mem, bucket)
}

func CountMinSketchDELETE(key string, mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	DELETE(key, mem, lru, bucket)
}



func CountMinSKetchMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	activeCMS := new(CountMinSketch)
	var activeKey string //kljuc CMS-a
	var userkey string //kljuc koji je korisnik uneo i koji se ispisuje korisniku
	userkey = ""
	for true {

		fmt.Println("=======================================")
		fmt.Print("Kljuc aktivnog CountMinSketch-a: ")
		fmt.Println(userkey)
		fmt.Println()
		fmt.Println("1 - Kreiraj CountMinSketch")
		fmt.Println("2 - Dobavi CountMinSketch iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Proveri broj ponavljanja")
		fmt.Println("5 - Upisi CountMinSketch u bazu podataka")
		fmt.Println("6 - Obrisi CountMinSketch iz baze podataka")
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
			found, tempKey, tempCms := CreateCountMinSketch(mem, lru, bucket)
			if found {
				fmt.Println("Vec postoji CountMinSKetch sa datim kljucem")
			} else {
				activeCMS = tempCms
				activeKey = tempKey
				userkey = activeKey[14:]
				fmt.Println("Uspesno kreiranje")
			}
			
		case "2":
			found, key, tempCMS := CountMinSketchGET(mem, lru, bucket)
			if found {
				activeCMS = tempCMS
				activeKey = key
				userkey = activeKey[14:]
				fmt.Println("Uspesno dobavljanje")
			} else {
				fmt.Println("Ne postoji CountMinSKetch sa datim kljucem")
			}
		case "3":

			if len(activeKey) != 0 {
				CountMinSketchAddElement(activeCMS)
				fmt.Println("Uspesno dodavanje")
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}

		case "4":
			if len(activeKey) != 0 {
				CountMinSketchCheckFrequency(activeCMS)
				fmt.Println("Operacija uspesna")
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}
		case "5":
			if len(activeKey) != 0 {
				CountMinSketchPUT(activeKey, activeCMS, mem, bucket)
				fmt.Println("Uspesan upis")
			} else{
				fmt.Println("Nije izabran aktivni CMS")
			}
		case "6":
			if len(activeKey) != 0 {
				CountMinSketchDELETE(activeKey, mem, lru, bucket)
				fmt.Println("Uspesno brisanje")
			} else{
				fmt.Println("Nije izabran aktivni CMS")
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