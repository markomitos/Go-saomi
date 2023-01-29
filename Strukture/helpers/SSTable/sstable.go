package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	. "project/gosaomi/bloom"
	. "project/gosaomi/dataType"
	. "project/gosaomi/wal"
)

// Treba ubaciti konfiguraciju
const INTERVAL = 10
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
	directory    string
	bloomFilter  *BloomFilter
	summary      *Summary
}

// Konstruktor
// size - ocekivani broj elemenata (velinica memtabele)
// directory - naziv direktorijuma
func NewSSTable(size uint32, directory string) *SSTable {
	sstable := new(SSTable)
	sstable.intervalSize = INTERVAL
	sstable.directory = directory
	sstable.bloomFilter = NewBloomFilter(size, FALSE_POSITIVE_RATE)
	sstable.summary = new(Summary)

	return sstable
}

// ------------- PAKOVANJE -------------

// Pakuje index u slog
func indexToByte(index *Index) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, index.offset)
	bytesKeySize := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesKeySize, index.keySize)
	bytes = append(bytes, bytesKeySize...)
	bytes = append(bytes, []byte(index.key)...)
	return bytes
}

// odpakuje niz bajtova u indeks
func byteToIndex(file *os.File, offset ...uint64) *Index {
	if len(offset) > 0 {
		file.Seek(int64(offset[0]), 0)
	}
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
	data := NewData(entry.Value, tombstone, timestamp)
	key := string(entry.Value)
	return key, data
}

// Priprema summary u niz bajtova za upis
func summaryToByte(summary *Summary) []byte {
	firstKeyLen := len([]byte(summary.firstKey))
	lastKeyLen := len([]byte(summary.lastKey))

	//HEADER --> velicina prvog elementa, velicina drugog elementa
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(firstKeyLen))
	bytesLastKeyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLastKeyLen, uint32(lastKeyLen))
	bytes = append(bytes, bytesLastKeyLen...)

	//GLAVNI DEO
	bytes = append(bytes, []byte(summary.firstKey)...)
	bytes = append(bytes, []byte(summary.lastKey)...)

	//TABELA INTERVALA
	for i := 0; i < len(summary.intervals); i++ {
		bytes = append(bytes, indexToByte(summary.intervals[i])...)
	}

	return bytes
}

// Cita summary iz summary fajla
func byteToSummary(file *os.File) *Summary {
	summary := new(Summary)
	summary.intervals = make([]*Index, 0)
	bytes := make([]byte, 4)

	//CITAMO HEADER
	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	firstKeyLen := binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	lastKeyLen := binary.BigEndian.Uint32(bytes)

	//CITAMO GLAVNI DEO
	bytes = make([]byte, firstKeyLen)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summary.firstKey = string(bytes)

	bytes = make([]byte, lastKeyLen)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summary.lastKey = string(bytes)

	//CITAMO NIZ INDEKSA
	index := new(Index)
	for true {
		index = byteToIndex(file)
		if index == nil {
			break
		}
		summary.intervals = append(summary.intervals, index)
	}

	return summary
}

// pomocne funkcije za konvertovanje niza bool-ova u niz bajtova
func boolsToBytes(t []bool) []byte {
	b := make([]byte, (len(t)+7)/8)
	for i, x := range t {
		if x {
			b[i/8] |= 0x80 >> uint(i%8)
		}
	}
	return b
}

func bytesToBools(b []byte) []bool {
	t := make([]bool, 8*len(b))
	for i, x := range b {
		for j := 0; j < 8; j++ {
			if (x<<uint(j))&0x80 == 0x80 {
				t[8*i+j] = true
			}
		}
	}
	return t
}

// Priprema bloom filtera za upis
func bloomFilterToByte(blm *BloomFilter) []byte {
	//Zapisujemo konstante
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(blm.K))

	bytesN := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesN, uint32(blm.N))
	bytes = append(bytes, bytesN...)

	bytesM := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesM, uint32(blm.M))
	bytes = append(bytes, bytesM...)

	//pretvaramo niz bool u bytes
	bitsetByte := boolsToBytes(blm.Bitset)

	//belezimo duzinu bitseta
	bytesBitSetLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesBitSetLen, uint32(len(bitsetByte)))
	bytes = append(bytes, bytesBitSetLen...)

	bytes = append(bytes, bitsetByte...)
	for _, fn := range blm.HashFuncs {
		//Belezimo duzinu svake hashfunkcije
		bytesHFLen := make([]byte, 4)
		binary.BigEndian.PutUint32(bytesHFLen, uint32(len(fn.Seed)))
		bytes = append(bytes, bytesHFLen...)

		//zapisuje hashfunkciju
		bytes = append(bytes, fn.Seed...)
	}

	return bytes
}

func byteToBloomFilter(file *os.File) *BloomFilter {
	blm := new(BloomFilter)
	bytes := make([]byte, 4)

	//Ucitavamo konstante
	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.K = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.N = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.M = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsetSize := binary.BigEndian.Uint32(bytes)

	//Ucitavamo bitset
	bytes = make([]byte, bitsetSize)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	blm.Bitset = bytesToBools(bytes)

	blm.HashFuncs = make([]HashWithSeed, 0)
	hashWithSeed := new(HashWithSeed)
	//Ucitavamo svaku hashfunkciju
	for i := uint32(0); i < blm.K; i++ {
		//Ucitavamo duzinu trenutne hf
		bytes = make([]byte, 4)
		_, err = file.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hashFuncLen := binary.BigEndian.Uint32(bytes)

		//citamo hf
		bytes = make([]byte, hashFuncLen)
		_, err = file.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hashWithSeed.Seed = bytes
		blm.HashFuncs = append(blm.HashFuncs, *hashWithSeed)
	}

	return blm
}

// Vraca pokazivace na kreirane fajlove(summary,index,data)
func (sstable *SSTable) makeFiles() (*os.File, *os.File, *os.File, *os.File) {
	//kreiramo novi direktorijum
	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		err = os.MkdirAll("files/sstable/"+sstable.directory, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Fajl vec postoji!")
	}

	//Kreiramo fajlove unutar direktorijuma
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	summary, err3 := os.Create(path + "/summary.bin")
	if err3 != nil {
		log.Fatal(err3)
	}

	index, err4 := os.Create(path + "/index.bin")
	if err4 != nil {
		log.Fatal(err4)
	}

	data, err5 := os.Create(path + "/data.bin")
	if err5 != nil {
		log.Fatal(err5)
	}

	filter, err6 := os.Create(path + "/filter.bin")
	if err6 != nil {
		log.Fatal(err6)
	}

	return summary, index, data, filter
}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data, index tabelu, summary
func (sstable *SSTable) Flush(keys []string, values []*Data) {
	//TO DO: dodati merkle stablo (metadata)

	summaryFile, indexFile, dataFile, filterFile := sstable.makeFiles()
	index := new(Index) //Pomocna struktura (menja se u svakoj iteraciji)
	summary := new(Summary)
	summary.firstKey = keys[0]
	summary.lastKey = keys[len(keys)-1]
	summary.intervals = make([]*Index, 0)

	offsetIndex := uint64(0) //offset ka indeksu(koristi se u summary)
	offsetData := uint64(0)  //offset ka disku(koristi se u indeks tabeli)

	intervalCounter := uint(sstable.intervalSize) //Kada dostigne postavljeni interval zapisuje novi offset indeksnog intervala
	for i := 0; i < len(keys); i++ {
		//Dodajemo u bloomFilter
		sstable.bloomFilter.AddToBloom([]byte(keys[i]))

		//Upisujemo trenutni podatak u data tabelu
		dataLen, err1 := dataFile.Write(dataToByte(keys[i], values[i]))
		if err1 != nil {
			log.Fatal(err1)
		}

		//upisujemo trenutni podatak u indeks tabelu
		index.key = keys[i]
		index.keySize = uint32(len([]byte(index.key)))
		index.offset = offsetData
		indexLen, err := indexFile.Write(indexToByte(index))
		if err != nil {
			log.Fatal(err)
		}

		if intervalCounter == sstable.intervalSize {
			index.offset = offsetIndex

			//Ubacimo u summary
			summary.intervals = append(summary.intervals, index)

			intervalCounter = 0
		}

		offsetData += uint64(dataLen)
		offsetIndex += uint64(indexLen)
		intervalCounter++
	}

	//Upis summary u summaryFile
	_, err2 := summaryFile.Write(summaryToByte(summary))
	if err2 != nil {
		log.Fatal(err2)
	}

	//Upis u bloomfilter fajl
	filterFile.Write(bloomFilterToByte(sstable.bloomFilter))

	//Zatvaranje fajlova
	summaryFile.Close()
	indexFile.Close()
	dataFile.Close()
	filterFile.Close()
}

// Printuje sstabelu
func (sstable *SSTable) ReadData() {
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	file, err := os.Open(path + "/data.bin")
	if err != nil {
		log.Fatal(err)
	}

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		entry.Print()
	}
	file.Close()
}

func (sstable *SSTable) ReadIndex() {
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	file, err := os.Open(path + "/index.bin")
	if err != nil {
		log.Fatal(err)
	}

	for {
		index := byteToIndex(file)
		if index == nil {
			break
		}
		fmt.Println(index)
	}
	file.Close()

}

func (sstable *SSTable) ReadSummary() {
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	file, err := os.Open(path + "/summary.bin")
	if err != nil {
		log.Fatal(err)
	}

	summary := byteToSummary(file)
	for i := 0; i < len(summary.intervals); i++ {
		fmt.Println(summary.intervals[i])
	}
	file.Close()

}
