package menu_functions

import (
	"fmt"
	. "project/keyvalue/structures/hll"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/token_bucket"
	"strconv"
)

//korisnik unosi kljuc i kreira se novi HLL
func CreateHyperLogLog(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *HLL) {
	var input string //kljuc
	hll := new(HLL)
	var precision uint8
	var tempInput string

	for true{

		fmt.Print("Unesite kljuc: ")
		input = GetKeyInput()
		if input == "*" {
			return true, input, nil
		}
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

				if choice == "*" {
					return true, input, nil
				}
				
				if choice == "1" {
					hll = BytesToHyperLogLog(data.Value)
					fmt.Println("Uspesno dobavljanje")
					return false, input, hll

				}else if choice == "2"{

					for true {
						fmt.Println("Unesite preciznost: ")
						n, err := fmt.Scanln(&tempInput)
						if tempInput == "*" {
							return true, input, nil
						}
						if err != nil {
							fmt.Println("Greska prilikom unosa: ", err)
						} else if n == 0 {
							fmt.Println("Prazan unos.  Molimo vas probajte opet.")
						}else if !IsNumeric(tempInput) {
							fmt.Println("Molimo vas unesite broj.")
						}else {
							tempInt, _ := strconv.ParseUint(tempInput, 10, 8)
							precision = uint8(tempInt)
							break
						}
				
					}

					hll, _ = NewHLL(precision)
					fmt.Println("Uspesno dodavanje")
					return false, input, hll
				} else{
					fmt.Println("Molimo vas unesite 1 ili 2")
				}
			}
			
			return true, input, nil
		}else{


			for true {
						fmt.Println("Unesite preciznost: ")
						n, err := fmt.Scanln(&tempInput)
						if tempInput == "*" {
							return true, input, nil
						}
						if err != nil {
							fmt.Println("Greska prilikom unosa")
						} else if n == 0 {
							fmt.Println("Prazan unos.  Molimo vas probajte opet.")
						}else if !IsNumeric(tempInput) {
							fmt.Println("Molimo vas unesite broj.")
						}else {
							tempInt, _ := strconv.ParseUint(tempInput, 10, 8)
							precision = uint8(tempInt)
							break
						}
				
					}
			hll, _ = NewHLL(precision)

			break
		}
	}
	fmt.Println()
	fmt.Println("Uspesno dodavanje")
	return false, input, hll
}

func GetHyperLogLog(mem MemTable, lru *LRUCache, bucket *TokenBucket) (bool, string, *HLL) {
	var key string
	hll := new(HLL)

	//unos
	key = GetKeyInput()
	if key == "*" {
		return false, key, nil
	}
	key = "HyperLogLog" + key
	
	found, data := GET(key, mem, lru, bucket)
	if found {
		hllBytes := data.Value
		hll = BytesToHyperLogLog(hllBytes)
		fmt.Println("Uspesno dobavljanje")
		return true, key, hll
	}
	return false, key, hll
}

func HyperLogLogAddElement(hll *HLL) {
	var val string

	//unos
	for true {
		fmt.Print("Unesite podatak koji zelite da dodate: ")
		_, err := fmt.Scanln(&val)
		if err != nil {
			fmt.Println("Greska prilikom unosa")
		} else if val == "*" {
			return
		} else {
			break
		}
	}
	
	hll.AddToHLL(val)
	fmt.Println("Uspesno dodavanje")
}

func HyperLogLogEstimate(hll *HLL) {

	estimation := hll.Estimate()
	fmt.Print("Procenjena kardinalnost iznosi: ")
	fmt.Print(estimation)
	
}

func HyperLogLogPUT(key string, hll *HLL, mem MemTable, bucket *TokenBucket) {
	byteshll := HyperLogLogToBytes(hll)
	PUT(key, byteshll, mem, bucket)
	fmt.Println("Uspesno dodavanje")
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
		fmt.Println("Velicina niza elemenata: ", activehll.M)
		fmt.Println("Preciznost: ", activehll.P)
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
			if !found {
				activehll = temphll
				activeKey = tempKey
				userkey = activeKey[11:]
			}

		case "2":
			found, key, temphll := GetHyperLogLog(mem, lru, bucket)
			if key != "*" {
				if found {
					activehll = temphll
					activeKey = key
					userkey = activeKey[11:]
					fmt.Println("Uspesno dobavljanje")
				} else {
					fmt.Println("Ne postoji HyperLogLog sa datim kljucem")
				}
			}
			
		case "3":

			if len(activeKey) != 0 {
				HyperLogLogAddElement(activehll)
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