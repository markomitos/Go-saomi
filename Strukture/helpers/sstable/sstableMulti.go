package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	. "project/gosaomi/bloom"
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
	merkle "project/gosaomi/merkle"
	. "project/gosaomi/wal"
)

type SSTableMulti struct {
	intervalSize uint
	directory    string
	bloomFilter  *BloomFilter
}

// ---------------- Konstruktor i inicijalizacija ----------------

// size - ocekivani broj elemenata (velinica memtabele)
// directory - naziv direktorijuma
func NewSSTableMulti(size uint32, directory string) *SSTableMulti {
	config := GetConfig()
	sstable := new(SSTableMulti)
	sstable.intervalSize = config.SStableInterval
	sstable.directory = directory

	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		sstable.bloomFilter = NewBloomFilter(size, config.BloomFalsePositiveRate)
	} else {
		sstable.LoadFilter()
	}

	return sstable
}

// Otvara trazenu datoteku od sstabele
func (sstable *SSTableMulti) OpenFile(filename string) *os.File {
	path, err2 := filepath.Abs("files/sstable/" + sstable.directory)
	if err2 != nil {
		log.Fatal(err2)
	}

	file, err := os.Open(path + "/" + filename)
	if err != nil {
		log.Fatal(err)
	}

	return file
}

// Ucitava podatke ukoliko vec postoji sstabela
func (sstable *SSTableMulti) LoadFilter() {
	//Ucitavamo bloomfilter
	filterFile := sstable.OpenFile("filter.bin")
	sstable.bloomFilter = byteToBloomFilter(filterFile)
	filterFile.Close()
}

// Vraca pokazivace na kreirane fajlove(summary,index,data, filter, metadata)
func (sstable *SSTableMulti) makeFiles() []*os.File {
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

	metadata, err7 := os.Create(path + "/metadata.txt")
	if err7 != nil {
		log.Fatal(err7)
	}

	files := make([]*os.File, 0)
	files = append(files,summary, index, data, filter, metadata)
	return files
}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data, index tabelu, summary
func (sstable *SSTableMulti) Flush(keys []string, values []*Data) {
	files := sstable.makeFiles()
	summaryFile, indexFile, dataFile, filterFile, metadataFile := files[0],files[1],files[2],files[3],files[4]
	summary := new(Summary)
	summary.FirstKey = keys[0]
	summary.LastKey = keys[len(keys)-1]
	summary.Intervals = make([]*Index, 0)

	offsetIndex := uint64(0) //Offset ka indeksu(koristi se u summary)
	offsetData := uint64(0)  //Offset ka disku(koristi se u indeks tabeli)

	nodes := make([]*merkle.Node, 0) //

	intervalCounter := uint(sstable.intervalSize) //Kada dostigne postavljeni interval zapisuje novi Offset indeksnog intervala
	for i := 0; i < len(keys); i++ {
		index := new(Index) //Pomocna struktura (menja se u svakoj iteraciji)

		//Dodajemo u bloomFilter
		sstable.bloomFilter.AddToBloom([]byte(keys[i]))

		//Dodajemo u merkle
		node := new(merkle.Node)
		node.Data = dataToByte(keys[i], values[i])
		nodes = append(nodes, node)

		//Upisujemo trenutni podatak u data tabelu
		dataLen, err1 := dataFile.Write(dataToByte(keys[i], values[i]))
		if err1 != nil {
			log.Fatal(err1)
		}

		//upisujemo trenutni podatak u indeks tabelu
		index.Key = keys[i]
		index.KeySize = uint32(len([]byte(index.Key)))
		index.Offset = offsetData
		indexLen, err := indexFile.Write(indexToByte(index))
		if err != nil {
			log.Fatal(err)
		}

		if intervalCounter == sstable.intervalSize {
			index.Offset = offsetIndex

			//Ubacimo u summary
			summary.Intervals = append(summary.Intervals, index)

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

	//Upis u metadata fajl
	merkleRoot := merkle.MakeMerkel(nodes)
	merkle.WriteFile(metadataFile, merkleRoot.Root)

	//Zatvaranje fajlova
	summaryFile.Close()
	indexFile.Close()
	dataFile.Close()
	filterFile.Close()
	metadataFile.Close()
}

// ------------ PRINTOVANJE ------------

func (sstable *SSTableMulti) ReadData() {
	file := sstable.OpenFile("data.bin")

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		entry.Print()
	}
	file.Close()
}

func (sstable *SSTableMulti) ReadIndex() {
	file := sstable.OpenFile("index.bin")

	for {
		index := byteToIndex(file)
		if index == nil {
			break
		}
		fmt.Println(index)
	}
	file.Close()
}

func (sstable *SSTableMulti) ReadSummary() *Summary {
	file := sstable.OpenFile("summary.bin")

	summary := byteToSummary(file)

	//U SLUCAJU DA NAM TREBA ISPIS
	// fmt.Println("First Key: ", summary.FirstKey)
	// fmt.Println("Last Key: ", summary.LastKey)
	// for i := 0; i < len(summary.Intervals); i++ {
	// 	fmt.Println(summary.Intervals[i])
	// }

	file.Close()

	return summary
}

func (sstable *SSTableMulti) ReadBloom() {
	file := sstable.OpenFile("filter.bin")

	blm := byteToBloomFilter(file)
	fmt.Println("K: ", blm.K)
	fmt.Println("N: ", blm.N)
	fmt.Println("M: ", blm.M)
	fmt.Println("Bitset: ", blm.Bitset)
	fmt.Println("hashfuncs: ", blm.HashFuncs)
	file.Close()

}

// ------------ PRETRAZIVANJE ------------

func (sstable *SSTableMulti) Find(Key string) (bool, *Data) {
	//Ucitavamo bloomfilter
	filterFile := sstable.OpenFile("filter.bin")
	sstable.bloomFilter = byteToBloomFilter(filterFile)
	filterFile.Close()

	//Proveravamo preko BloomFiltera da li uopste treba da pretrazujemo
	if !sstable.bloomFilter.IsInBloom([]byte(Key)) {
		return false, nil
	}

	//Proveravamo da li je kljuc van opsega
	summary := sstable.ReadSummary()

	if Key < summary.FirstKey || Key > summary.LastKey {
		return false, nil
	}

	indexInSummary := new(Index)
	found := false
	for i := 1; i < len(summary.Intervals); i++ {
		if Key < summary.Intervals[i].Key {
			indexInSummary = summary.Intervals[i-1]
			found = true
			break
		}
	}
	if !found {
		indexInSummary = summary.Intervals[len(summary.Intervals)-1]
	}

	// ------ Otvaramo index tabelu ------
	indexFile := sstable.OpenFile("index.bin")

	found = false
	indexFile.Seek(int64(indexInSummary.Offset), 0) //Pomeramo pokazivac na pocetak trazenog indeksnog dela
	currentIndex := new(Index)

	//trazimo redom
	for i := 0; i < int(sstable.intervalSize); i++ {
		currentIndex = byteToIndex(indexFile)
		if currentIndex.Key == Key {
			found = true
			break
		}
	}
	indexFile.Close() //zatvaramo indeksnu tabelu

	if !found {
		return false, nil
	}

	// ------ Pristupamo disku i uzimamo podtak ------
	dataFile := sstable.OpenFile("data.bin")
	_, foundData := ByteToData(dataFile, currentIndex.Offset)
	dataFile.Close()

	return true, foundData
}

// ------------ DOBAVLJANJE PODATAKA ------------
//Cita sve podatke i vraca 2 niza
func (sstable *SSTableMulti) GetData() ([]string, []*Data){
	file := sstable.OpenFile("data.bin")
	keys := make([]string,0)
	dataArray := make([]*Data, 0)

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		keys = append(keys, string(entry.Key))
		data := new(Data)
		data.Value = entry.Value
		//Tombstone
		data.Tombstone = false
		if entry.Tombstone[0] == byte(uint8(1)) {
			data.Tombstone = true
		}
		data.Timestamp = binary.BigEndian.Uint64(entry.Timestamp)
		dataArray = append(dataArray, data)
	}
	file.Close()

	return keys, dataArray
}

//Otvara fajl i postavlja pokazivac na pocetak data zone
//vraca pokazivac na taj fajl i velicinu data zone
func (sstable *SSTableMulti) GoToData()  (*os.File, uint64){
	file := sstable.OpenFile("data.bin")
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return file, uint64(fileInfo.Size())
}