package menu_functions

import (
	"fmt"
	. "project/keyvalue/structures/hll"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/token_bucket"
)

//korisnik unosi kljuc i kreira se novi HLL
func CreateHyperLogLog(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *HLL) {
	var input string //kljuc
	hll := new(HLL)
	var precision uint8

	for true{

		fmt.Print("Unesite kljuc: ")
		fmt.Scanln(&input)
		input = "HyperLogLog" + input
		found, data := GET(input, mem, lru, bucket)
		if found == true {
			var choice string

			for true {

				fmt.Println("Takav kljuc vec postoji u bazi podataka. Da li zelite da:")
				fmt.Println("1. Dobavite ovaj HyperLogLog iz baze podataka")
				fmt.Println("2. Napravite novi HyperLogLog pod ovim kljucem")
				fmt.Print("Unesite 1 ili 2: ")
				fmt.Scanln(&choice)

				if choice == "1" {
					hll = BytesToHyperLogLog(data.Value)
					return false, input, hll

				}else if choice == "2"{

					fmt.Print("Unesite preciznost: ")
					fmt.Scanln(&precision)

					hll, _ = NewHLL(precision)
					return false, input, hll
				} else{
					fmt.Println("Molimo vas unesite 1 ili 2")
				}
			}
			
			return true, input, nil
		}else{

			//TODO: dodaj validacije
			fmt.Print("Unesite preciznost: ")
			fmt.Scanln(&precision)
			hll, _ = NewHLL(precision)

			break
		}
	}
	fmt.Println()
	return false, input, hll
}

func GetHyperLogLog(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *HLL) {
	var key string
	hll := new(HLL)

	//unos
	fmt.Print("Unesite kljuc: ")
	fmt.Scanln(&key)
	key = "HyperLogLog" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		hllBytes := data.Value
		hll = BytesToHyperLogLog(hllBytes)
		return true, key, hll
	}
	return false, key, hll
}

func HyperLogLogAddElement(hll *HLL) {
	var val string

	//unos
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)
	hll.AddToHLL(val)
}

func HyperLogLogEstimate(hll *HLL) {

	estimation := hll.Estimate()
	fmt.Print("Procenjena kardinalnost iznosi: ")
	fmt.Print(estimation)
	
}

func HyperLogLogPUT(key string, hll *HLL, mem MemTable, bucket *TokenBucket) {
	byteshll := HyperLogLogToBytes(hll)
	PUT(key, byteshll, mem, bucket)
}

func HyperLogLogMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	activehll := new(HLL)
	var activeKey string //kljuc HyperLogLog-a
	var userkey string   //kljuc koji je korisnik uneo i koji se ispisuje korisniku
	userkey = ""
	for true {

		fmt.Println("=======================================")
		fmt.Print("Kljuc aktivnog HyperLogLog-a: ")
		fmt.Println(userkey)
		fmt.Println()
		fmt.Println("1 - Kreiraj HyperLogLog")
		fmt.Println("2 - Dobavi HyperLogLog iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Proceni kardinalnost")
		fmt.Println("5 - Upisi HyperLogLog u bazu podataka")
		fmt.Println("6 - Obrisi HyperLogLog iz baze podataka")
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
			found, tempKey, temphll := CreateHyperLogLog(mem, lru, bucket)
			if found {
				fmt.Println("Vec postoji HyperLogLog sa datim kljucem")
			} else {
				activehll = temphll
				activeKey = tempKey
				userkey = activeKey[11:]
				fmt.Println("Uspesno kreiranje")
			}

		case "2":
			found, key, temphll := GetHyperLogLog(mem, lru, bucket)
			if found {
				activehll = temphll
				activeKey = key
				userkey = activeKey[11:]
				fmt.Println("Uspesno dobavljanje")
			} else {
				fmt.Println("Ne postoji HyperLogLog sa datim kljucem")
			}
		case "3":

			if len(activeKey) != 0 {
				HyperLogLogAddElement(activehll)
				fmt.Println("Uspesno dodavanje")
			} else {
				fmt.Println("Nije izabran aktivni HyperLogLog")
			}

		case "4":
			if len(activeKey) != 0 {
				HyperLogLogEstimate(activehll)
			} else {
				fmt.Println("Nije izabran aktivni HyperLogLog")
			}
		case "5":
			if len(activeKey) != 0 {
				HyperLogLogPUT(activeKey, activehll, mem, bucket)
				fmt.Println("Uspesan upis")
			} else {
				fmt.Println("Nije izabran aktivni HyperLogLog")
			}
		case "6":
			if len(activeKey) != 0 {
				DELETE(activeKey, mem, lru, bucket)
				fmt.Println("Uspesno brisanje")
			} else {
				fmt.Println("Nije izabran aktivni HyperLogLog")
				
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