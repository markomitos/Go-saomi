package menu_functions

import (
	"fmt"
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