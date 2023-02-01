package menu_functions

import (
	. "project/gosaomi/dataType"
	. "project/gosaomi/entry"
	. "project/gosaomi/least_reacently_used"
	. "project/gosaomi/lsm"
	. "project/gosaomi/memtable"
	. "project/gosaomi/scan"
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
func DELETE(key string, memtable MemTable, bucket *TokenBucket) bool {
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
	memtable.Remove(key)

	//TO DO: Brisemo u cache-u
	
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

// RANGE SCAN
func RangeScan(minKey string, maxKey string, pageLen uint32, pageNum uint32) (bool, []string, []*Data){
	lsm := ReadLsm()
	scan := NewScan(pageLen, pageNum)

	//update-a se scan
	lsm.RangeScan(minKey, maxKey, scan)

	if len(scan.Keys) == 0{
		return false, nil, nil
	}

	return true, scan.Keys, scan.Data
}