package menu_functions

import (
	"fmt"
	"os"
	. "project/gosaomi/cms"
	. "project/gosaomi/dataType"
	. "project/gosaomi/entry"
	. "project/gosaomi/least_reacently_used"
	. "project/gosaomi/lsm"
	. "project/gosaomi/memtable"
	. "project/gosaomi/scan"
	. "project/gosaomi/simhash"
	. "project/gosaomi/token_bucket"
	. "project/gosaomi/wal"
	"time"
)

//TO DO: funkcija koja ce se pozivati iz menija

// ------------ WRITEPATH ------------
// Upisuje podatak u bazu i vraca da li je operacija bila uspesna
func PUT(key string, data *Data, memtable MemTable, bucket *TokenBucket) bool {
	if !bucket.Take() {
		return false
	} 

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

// ------------ RANGE SCAN ------------
// vraca niz kljuceva i niz podataka koji su u opsegu datog intervala
// Vraca rezultate u opsegu od najnovijeg do najstarijeg
// To postize tako sto iterira od najnovije do najstarije sstabele
func RANGE_SCAN(minKey string, maxKey string, pageLen uint32, pageNum uint32, memtable MemTable) (bool, []string, []*Data){
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

// ------------ LIST SCAN ------------
// vraca niz kljuceva i niz podataka koji pocinju datim prefiksom
// Vraca rezultate u opsegu od najnovijeg do najstarijeg
// To postize tako sto iterira od najnovije do najstarije sstabele
func LIST_SCAN(prefix string, pageLen uint32, pageNum uint32, memtable MemTable) (bool, []string, []*Data){
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

//Ukoliko se kljucevi nalaze u datoteci poredi ih i vraca hemingovo rastojanje izmedju vrednosti
func SimHashCompare(key1 string, key2 string, mem MemTable, lru *LRUCache, bucket *TokenBucket) (int) {
	found1, data1 := GET(key1, mem, lru, bucket)
	found2, data2 := GET(key2, mem, lru, bucket)

	if found1 == false {
		fmt.Println("Kljuc 1 se ne nalazi u memoriji.")
		return -1
	}

	if found2 == false {
		fmt.Println("Kljuc 2 se ne nalazi u memoriji.")
		return -1
	}

	return Compare(data1.Value, data2.Value)

}

func CreateCountMinSketch(mem MemTable, lru *LRUCache, bucket *TokenBucket) (string, *CountMinSketch) {
	var input string
	cms := new(CountMinSketch)
	for true{

		fmt.Print("Unesite kljuc: ")
		fmt.Scanln(&input)
		input = "CountMinSketch" + input
		found, _ := GET(input, mem, lru, bucket)
		if found == true {
			fmt.Println("Takav kljuc vec postoji u bazi podataka. Molimo vas unesite drugi.")
		}else {
			var epsilon float64
			var delta float64

			//TODO: dodaj validacije
			fmt.Print("Unesite preciznost (epsilon): ")
			fmt.Scanln(&epsilon)
			fmt.Print("Unesite sigurnost tacnosti (delta): ")
			fmt.Scanln(&delta)
			cms = NewCountMinSketch(epsilon, delta)

			break
		}
	}

	return input, cms
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
	fmt.Print("Unesite podatak koji zelite da dodate: ")
	fmt.Scanln(&val)

	freq := CheckFrequencyInCms(cms, val)

	fmt.Print("Broj ponavljanja podatka iznosi: ")
	fmt.Println(freq)
}

func CountMinSketchPUT(key string, cms *CountMinSketch, mem MemTable, bucket *TokenBucket, tombstone bool) {
	data := new(Data)
	bytesCms := CountMinSkechToBytes(cms)
	data.Value = bytesCms
	data.Timestamp = uint64(time.Now().Unix())
	data.Tombstone = tombstone
	PUT(key, data, mem, bucket)
}



func CountMinSKetchMenu(mem MemTable, lru *LRUCache, bucket *TokenBucket) {
	activeCMS := new(CountMinSketch)
	var activeKey string
	for true {
		fmt.Println("1 - Kreiraj CountMinSketch")
		fmt.Println("2 - Dobavi CountMinSketch iz baze podataka")
		fmt.Println("3 - Dodaj element")
		fmt.Println("4 - Proveri broj ponavljanja")
		fmt.Println("5 - Upisi CountMinSketch u bazu podataka")
		fmt.Println("6 - Obrisi CountMinSketch iz baze podataka")
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
			activeKey, activeCMS = CreateCountMinSketch(mem, lru, bucket)
		case "2":
			found, key, tempCMS := CountMinSketchGET(mem, lru, bucket)
			if found {
				activeCMS = tempCMS // da li ovo ovako radi ili ipak mora deep copy?
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