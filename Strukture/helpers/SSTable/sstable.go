package sstable

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	. "structures/bloom/bloomfilter"
	. "structures/data"
	. "structures/wal"
)

// Treba ubaciti konfiguraciju
const INTERVAL = 128
const FALSE_POSITIVE_RATE = 2

type Index struct {
	offset  uint64
	keySize uint32 //Ovo cuva velicinu kljuca i pomocu toga citamo iz fajla
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
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, index.offset)
	binary.BigEndian.AppendUint32(bytes, index.keySize)
	bytes = append(bytes, []byte(index.key)...)
	return bytes
}

// odpakuje niz bajtova u indeks
func byteToIndex(file *os.File, offset uint64) *Index {
	file.Seek(int64(offset), 0)
	bytes := make([]byte, 12) //pravimo mesta za offset(8) i keysize(4)
	_, err := file.Read(bytes)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Fatal(err)
	}

	//citamo ucitane vrednosti
	index := new(Index)
	index.offset = binary.BigEndian.Uint64(bytes[0:8])
	index.keySize = binary.BigEndian.Uint32(bytes[8:12])

	//citamo kljuc
	keyBytes := make([]byte, index.keySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		log.Fatal(err)
	}
	index.key = string(keyBytes)

	return index
}

// Pakuje kljuc-vrednost i ostale podatke u niz bajtova za zapis na disku
func dataToByte(key string, data *Data) []byte {
	//izracunaj duzinu kljuca i vrednosti
	Key_size := make([]byte, 8)
	Value_size := make([]byte, 8)
	binary.BigEndian.PutUint64(Key_size, uint64(int64(len([]byte(key)))))
	binary.BigEndian.PutUint64(Value_size, uint64(int64(len(data.Value))))

	keyBytes := []byte(key)
	valueBytes := data.Value
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, data.Timestamp)

	//Tombstone
	tombstoneBytes := make([]byte, 0)
	if data.Tombstone {
		tombstoneBytes = append(tombstoneBytes, uint8(1))
	} else {
		tombstoneBytes = append(tombstoneBytes, uint8(0))
	}

	//ubaci sve u niz bajtova da bi napravio Crc
	bytes := make([]byte, 0)
	bytes = append(bytes, timestampBytes...)
	bytes = append(bytes, tombstoneBytes...)
	bytes = append(bytes, Key_size...)
	bytes = append(bytes, Value_size...)
	bytes = append(bytes, keyBytes...)
	bytes = append(bytes, valueBytes...)
	Crc := make([]byte, 4)
	binary.BigEndian.PutUint32(Crc, uint32(CRC32(bytes)))

	returnBytes := Crc                          //Prvih 4 bajta
	returnBytes = append(returnBytes, bytes...) //Ostali podaci

	return returnBytes
}

// Odpakuje sa zapisa na disku u podatak
func ByteToData(file *os.File, offset uint64) (string, *Data) {
	file.Seek(int64(offset), 0)
	entry := ReadEntry(file)

	//Tombstone
	tombstone := false
	if entry.Tombstone[0] == byte(uint8(1)) {
		tombstone = true
	}
	timestamp := binary.BigEndian.Uint64(entry.Timestamp)
	data := newData(entry.Value, tombstone, timestamp)
	key := string(entry.Value)
	return key, data

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
