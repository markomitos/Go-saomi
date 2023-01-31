package writepath

import (
	. "project/gosaomi/dataType"
	. "project/gosaomi/memtable"
	. "project/gosaomi/token_bucket"
	. "project/gosaomi/wal"
)

//Upisuje podatak u bazu i vraca da li je operacija bila uspesna
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