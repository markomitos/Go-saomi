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
)

type SSTableSingle struct {
	intervalSize uint
	directory    string
	bloomFilter  *BloomFilter
}

func NewSSTableSingle(size uint32, directory string) *SSTableSingle {
	config := GetConfig()
	sstable := new(SSTableSingle)
	sstable.intervalSize = config.SStableInterval
	sstable.directory = directory

	_, err := os.Stat("files/sstable/" + sstable.directory)
	if os.IsNotExist(err) {
		sstable.bloomFilter = NewBloomFilter(size, config.BloomFalsePositiveRate)
	} else {
		// sstable.LoadFilter()
	}

	return sstable
}

// Vraca pokazivace na kreirane fajlove(summary,index,data, filter, metadata)
func (sstable *SSTableSingle) makeFiles() (*os.File, *os.File) {
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

	sstableFile, err3 := os.Create(path + "/sstable.bin")
	if err3 != nil {
		log.Fatal(err3)
	}

	metadata, err7 := os.Create(path + "/metadata.txt")
	if err7 != nil {
		log.Fatal(err7)
	}

	return sstableFile, metadata
}

// Iterira se kroz string kljuceve i ubacuje u:
// Bloomfilter
// zapisuje u data, index tabelu, summary
func (sstable *SSTableSingle) Flush(keys []string, values []*Data) {
	sstableFile, metadataFile := sstable.makeFiles()
	summary := new(Summary)
	summary.FirstKey = keys[0]
	summary.LastKey = keys[len(keys)-1]
	summary.Intervals = make([]*Index, 0)

	offsetIndex := uint64(0) //Relativan offset ka indeksu(koristi se u summary)
	offsetData := uint64(0)  //Relativan offset ka disku(koristi se u indeks tabeli)

	nodes := make([]*merkle.Node, 0)

	//Cuva podatke o data zoni
	dataBytes := make([]byte, 0)

	//Cuva podatke o index zoni
	indexBytes := make([]byte, 0)
	indexLen := 0

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
		tempData := dataToByte(keys[i], values[i])
		dataBytes = append(dataBytes, tempData...)
		dataLen := len(tempData)

		//upisujemo trenutni podatak u indeks tabelu
		index.Key = keys[i]
		index.KeySize = uint32(len([]byte(index.Key)))
		index.Offset = offsetData
		tempIndex := indexToByte(index)
		indexBytes = append(indexBytes, tempIndex...)
		indexLen := len(tempIndex)

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

	summaryBytes := summaryToByte(summary)

	// ------------ HEADER ------------
	// --> 3*uint64 = 24B

	// 1. element je velicina data zone
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(len(dataBytes)))

	// 2. element je velicina indeksne zone
	bytesTemp := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(len(indexBytes)))
	bytes = append(bytes, bytesTemp...)

	// 3. element je velicina summary zone
	bytesTemp = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(len(summaryBytes)))
	bytes = append(bytes, bytesTemp...)

	//Upisujemo header u fajl
	_, err := sstableFile.Write(bytes)
	if err != nil {
		log.Fatal(err)
	}

	//DATA
	_, err = sstableFile.Write(dataBytes)
	if err != nil {
		log.Fatal(err)
	}

	//INDEX
	_, err = sstableFile.Write(indexBytes)
	if err != nil {
		log.Fatal(err)
	}

	//SUMMARY
	_, err = sstableFile.Write(summaryBytes)
	if err != nil {
		log.Fatal(err)
	}

	//FILTER
	_, err = sstableFile.Write(bloomFilterToByte(sstable.bloomFilter))
	if err != nil {
		log.Fatal(err)
	}

	//Upis u metadata fajl
	merkleRoot := merkle.MakeMerkel(nodes)
	merkle.WriteFile(metadataFile, merkleRoot.Root)

	//Zatvaranje fajlova
	sstableFile.Close()
	metadataFile.Close()
}
