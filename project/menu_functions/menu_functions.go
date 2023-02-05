package menu_functions

import (
	"fmt"
	. "project/keyvalue/structures/dataType"
	. "project/keyvalue/structures/entry"
	. "project/keyvalue/structures/least_reacently_used"
	. "project/keyvalue/structures/lsm"
	. "project/keyvalue/structures/memtable"
	. "project/keyvalue/structures/scan"
	. "project/keyvalue/structures/token_bucket"
	. "project/keyvalue/structures/wal"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//Ukoliko string ima samo cifre vraca true
func IsNumeric(word string) bool{
	return regexp.MustCompile(`\d`).MatchString(word)
}

func GetKeyInput() (string) {
	var key string
	for true {
		fmt.Print("Unesite kljuc: ")
		n, err := fmt.Scanln(&key)

		//validacije rezervisanih reci
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos. Molimo vas probajte opet.")
		}else if strings.HasPrefix(key, "BloomFilter") {
			print("Upotrebili ste rezervisani prefix! Molimo vas unesite drugi kljuc.")
		} else if strings.HasPrefix(key, "CountMinSketch") {
			print("Upotrebili ste rezervisani prefix! Molimo vas unesite drugi kljuc.")
		} else if strings.HasPrefix(key, "HyperLogLog") {
			print("Upotrebili ste rezervisani prefix! Molimo vas unesite drugi kljuc.")
		} else if strings.HasPrefix(key, "SimHash") {
			print("Upotrebili ste rezervisani prefix! Molimo vas unesite drugi kljuc.")
		} else if key == "*" {
			print("Upotrebili ste rezervisani kljuc! Molimo vas unesite drugi kljuc.")
		}else {
			break
		}
		
	}
	return key
}

func GetValueInput() ([]byte) {
	var elem []byte
	for true {
		fmt.Print("Unesite vrednost: ")
		n, err := fmt.Scanln(&elem)
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos. Molimo vas probajte opet.")
		} else {
			break
		}
	}
	return elem
}

func GetUserInput() (string, []byte){

	key := GetKeyInput()
	if key != "*" {
		elem := GetValueInput()
		return key, elem
	}
	return key, nil

}

// ------------ WRITEPATH ------------
// Upisuje podatak u bazu i vraca da li je operacija bila uspesna
func PUT(key string, value []byte, memtable MemTable, bucket *TokenBucket) bool {
	if !bucket.Take() {
		return false
	} 

	//PRAVIMO DATA ZA UPIS
	data:= new(Data)
	data.Value = value
	data.Timestamp = uint64(time.Now().Unix()) //upisuje se trenutno vreme
	data.Tombstone = false

	//UPISUJEMO U WAL
	wal := NewWriteAheadLog("files/wal")
	entry := NewEntry(key, data)
	wal.WriteEntry(entry)

	//UPISEMO U OM -> MEMTABLE
	memtable.Put(key, data)

	return true
}

//Logicko brisanje
func DELETE(key string, memtable MemTable,lru *LRUCache, bucket *TokenBucket) bool {
	if !bucket.Take() {
		return false
	} 
	//UPISUJEMO U WAL kao obrisan
	data:= new(Data)
	data.Timestamp = uint64(time.Now().Unix())
	data.Tombstone = true
	data.Value = make([]byte, 0) //Posto je obrisan necemo cuvati vrednost

	wal := NewWriteAheadLog("files/wal")
	entry := NewEntry(key, data)
	wal.WriteEntry(entry)


	//Brisemo u memtable-u
	//Ukoliko se ne nalazi u OM poslace se novi put zahtev automatski
	memtable.Remove(key)

	//Brisemo u cache-u
	lru.Delete(key)

	return true
}

// ------------ READPATH ------------
// Cita podatak i ukoliko je uspesno citanje smesta ga u cache
func GET(key string, memtable MemTable,lru *LRUCache, bucket *TokenBucket) (bool, *Data){
	if !bucket.Take() {
		return false, nil
	} 

	//1. Proveravamo memtable
	found, data := memtable.Find(key)
	if found{
		if data.Tombstone == false{
			//Dodajemo u cache
			lru.Set(key, data)

			return true, data
		} else {
			return false, nil
		}
	}

	//2. Proveravamo Cache
	found, data = lru.Get(key)
	if found {
		return true, data
	}

	//3. Proveravamo sstabele
	lsm := ReadLsm()
	found, data = lsm.Find(key)
	if found{
		if data.Tombstone == false{
			//Dodajemo u cache
			lru.Set(key, data)

			return true, data
		} else {
			return false, nil
		}
	}
	return false, nil
}

//Funkcija koja uzima unos korisnika i poziva RangeScan
func InitiateRangeScan(mem MemTable) {
	var minKey string
	var maxKey string
	var pageLen uint32
	var pageNum uint32

	var tempInput string

	for true {
		fmt.Println("Unesite najmanji kljuc: ")
		n, err := fmt.Scanln(&minKey)
		if minKey == "*" {
			return
		}

		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		} else {
			break
		}
	}
	for true {
		fmt.Println("Unesite najveci kljuc: ")
		n, err := fmt.Scanln(&maxKey)
		if maxKey == "*" {
			return
		}

		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		} else {
			break
		}
	}
	for true {
		fmt.Println("Unesite velicinu stranice: ")
		n, err := fmt.Scanln(&tempInput)
		if tempInput == "*" {
			return
		}
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		}else if !IsNumeric(tempInput) {
			fmt.Println("Molimo vas unesite broj.")
		}else {
			tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
			pageLen = uint32(tempInt)
			break
		}

	}
	for true {
		fmt.Println("Unesite broj stranice: ")
		n, err := fmt.Scanln(&tempInput)
		if tempInput == "*" {
			return
		}
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		}else if !IsNumeric(tempInput) {
			fmt.Println("Molimo vas unesite broj.")
		}else {
			tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
			pageNum = uint32(tempInt)
			break
		}
	}
	found, keys, datas := RANGE_SCAN(minKey, maxKey, pageLen, pageNum, mem, bucket)
	if found {
		fmt.Println("=======================================")
		fmt.Println("========== REZULTAT PRETRAGE ==========")
		fmt.Println("=======================================")
		for i := 0; i < len(keys); i++ {
			fmt.Println("Kljuc: ", keys[i])
			datas[i].Print()
		}
		fmt.Println("=======================================")
	} else {
		fmt.Println("Trazena stranica ne postoji")
	}
}

// ------------ RANGE SCAN ------------
// vraca niz kljuceva i niz podataka koji su u opsegu datog intervala
// Vraca rezultate u opsegu od najnovijeg do najstarijeg
// To postize tako sto iterira od najnovije do najstarije sstabele
func RANGE_SCAN(minKey string, maxKey string, pageLen uint32, pageNum uint32, memtable MemTable, bucket *TokenBucket) (bool, []string, []*Data){
	if !bucket.Take() {
		return false, nil, nil
	} 

	lsm := ReadLsm()
	scan := NewScan(pageLen, pageNum)

	//Trazimo prvo u memtabeli
	memtable.RangeScan(minKey, maxKey, scan)
	if scan.FoundResults < scan.SelectedPageEnd{
		//Trazimo u svim sstabelama i azuriramo scan nakon svakog poklapanja
		lsm.RangeScan(minKey, maxKey, scan)
	}

	if len(scan.Keys) == 0{
		return false, nil, nil
	}

	return true, scan.Keys, scan.Data
}

//Funkcija koja uzima unos korisnika i poziva ListScan
func InitiateListScan(mem MemTable) {
	var prefix string
	var pageLen uint32
	var pageNum uint32
	var tempInput string

	for true {
		fmt.Println("Unesite prefix: ")
		n, err := fmt.Scanln(&prefix)
		if prefix == "*" {
			return
		}

		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		} else {
			break
		}
	}
	for true {
		fmt.Println("Unesite velicinu stranice: ")
		n, err := fmt.Scanln(&tempInput)
		if tempInput == "*" {
			return
		}
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		}else if !IsNumeric(tempInput) {
			fmt.Println("Molimo vas unesite broj.")
		}else {
			tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
			pageLen = uint32(tempInt)
			break
		}

	}
	for true {
		fmt.Println("Unesite broj stranice: ")
		n, err := fmt.Scanln(&tempInput)
		if tempInput == "*" {
			return
		}
		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
		}else if !IsNumeric(tempInput) {
			fmt.Println("Molimo vas unesite broj.")
		}else {
			tempInt, _ := strconv.ParseUint(tempInput, 10, 64)
			pageNum = uint32(tempInt)
			break
		}
	}
	found, keys, datas := LIST_SCAN(prefix, pageLen, pageNum, mem, bucket)
	if found {
		fmt.Println("=======================================")
		fmt.Println("========== REZULTAT PRETRAGE ==========")
		fmt.Println("=======================================")
		for i := 0; i < len(keys); i++ {
			print("Kljuc: ", keys[i])
			datas[i].Print()
		}
		fmt.Println("=======================================")
	}else {
		fmt.Println("Trazena stranica ne postoji")
	}
}

// ------------ LIST SCAN ------------
// vraca niz kljuceva i niz podataka koji pocinju datim prefiksom
// Vraca rezultate u opsegu od najnovijeg do najstarijeg
// To postize tako sto iterira od najnovije do najstarije sstabele
func LIST_SCAN(prefix string, pageLen uint32, pageNum uint32, memtable MemTable, bucket *TokenBucket) (bool, []string, []*Data){
	if !bucket.Take() {
		return false, nil, nil
	} 
	
	lsm := ReadLsm()
	scan := NewScan(pageLen, pageNum)

	//Trazimo prvo u memtabeli
	memtable.ListScan(prefix, scan)
	if scan.FoundResults < scan.SelectedPageEnd{
		//Trazimo u svim sstabelama i azuriramo scan nakon svakog poklapanja
		lsm.ListScan(prefix, scan)
	}

	if len(scan.Keys) == 0{
		return false, nil, nil
	}

	return true, scan.Keys, scan.Data
}

func TimestampToTime(timestamp uint64) time.Time {
	time := time.Unix(int64(timestamp), 0)
	return time
}


