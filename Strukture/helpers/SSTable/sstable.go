package sstable

import (
	"encoding/binary"
	. "structures/bloom/bloomfilter"
	. "structures/data"
)

// Treba ubaciti konfiguraciju
const INTERVAL = 128
const FALSE_POSITIVE_RATE = 2

type Index struct {
	offset  uint64
	keySize uint //Ovo cuva velicinu kljuca i pomocu toga citamo iz fajla
	key     string
}

type Summary struct {
	firstKey  string
	lastKey   string
	intervals []*Index
}

type SSTable struct {
	intervalSize uint
	bloomFilter  *BloomFilter
	summary      *Summary
}

// Konstruktor
func NewSSTable(size uint) *SSTable {
	sstable := new(SSTable)
	sstable.intervalSize = INTERVAL
	sstable.bloomFilter = NewBloomFilter(size, FALSE_POSITIVE_RATE)
	sstable.summary = new(Summary)
	//pravljenje fajlova
	return sstable
}

// Pakuje index u slog
func indexToByte(index *Index) []byte {
	bytes := make([]byte, 8) //prostor za offset
	binary.BigEndian.PutUint64(bytes, index.offset)

	binary.BigEndian.AppendUint32()
}

// odpakuje niz bajtova u indeks
func byteToIndex() {

}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data
// Kreira summary i index deo
func (sstable *SSTable) Flush(keys []string, values []Data) {
	intervalCounter := uint(0)
	for i := 0; i < len(keys); i++ {
		intervalCounter++
		//Dodajemo u bloomFilter
		sstable.bloomFilter.AddToBloom([]byte(keys[i]))
		if intervalCounter == sstable.intervalSize {
			//Ubacimo u summary
			intervalCounter = 0
		}
	}
}
