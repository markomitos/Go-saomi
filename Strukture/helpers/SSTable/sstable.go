package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	. "project/gosaomi/bloom"
	. "project/gosaomi/cms"
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
	. "project/gosaomi/entry"
	. "project/gosaomi/hll"
	. "project/gosaomi/scan"
)

type SST interface {
	makeFiles() []*os.File
	Flush(keys []string, values []*Data)
	Find(key string) (bool, *Data)
	GoToData() (*os.File, uint64)
	RangeScan(minKey string, maxKey string, scan *Scan)
	ReadData()
}

type Index struct {
	Offset  uint64
	KeySize uint32 //Ovo cuva velicinu kljuca i pomocu toga citamo iz fajla
	Key     string
}

type Summary struct {
	FirstKey  string
	LastKey   string
	Intervals []*Index
}

func NewSSTable(size uint32, directory string) SST {
	config := GetConfig()
	var sstable SST
	if config.SSTableFileConfig == "single" {
		sstable = NewSSTableSingle(size, directory)
	} else if config.SSTableFileConfig == "multiple" {
		sstable = NewSSTableMulti(size, directory)
	}

	return sstable
}

// ------------- PAKOVANJE -------------

// Pakuje index u slog
func indexToByte(index *Index) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, index.Offset)
	bytesKeySize := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesKeySize, index.KeySize)
	bytes = append(bytes, bytesKeySize...)
	bytes = append(bytes, []byte(index.Key)...)
	return bytes
}

// odpakuje niz bajtova u indeks
func byteToIndex(file *os.File, Offset ...uint64) *Index {
	if len(Offset) > 0 {
		file.Seek(int64(Offset[0]), 0)
	}
	bytes := make([]byte, 12) //pravimo mesta za Offset(8) i keysize(4)
	_, err := file.Read(bytes)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Fatal(err)
	}

	//citamo ucitane vrednosti
	index := new(Index)
	index.Offset = binary.BigEndian.Uint64(bytes[0:8])
	index.KeySize = binary.BigEndian.Uint32(bytes[8:12])

	//citamo kljuc
	keyBytes := make([]byte, index.KeySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		log.Fatal(err)
	}
	index.Key = string(keyBytes)

	return index
}

// Pakuje kljuc-vrednost i ostale podatke u niz bajtova za zapis na disku
func dataToByte(Key string, data *Data) []byte {
	//izracunaj duzinu kljuca i vrednosti
	Key_size := make([]byte, 8)
	Value_size := make([]byte, 8)
	binary.BigEndian.PutUint64(Key_size, uint64(int64(len([]byte(Key)))))
	binary.BigEndian.PutUint64(Value_size, uint64(int64(len(data.Value))))

	keyBytes := []byte(Key)
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
	binary.BigEndian.PutUint32(Crc, uint32(crc32.ChecksumIEEE(bytes)))

	returnBytes := Crc                          //Prvih 4 bajta
	returnBytes = append(returnBytes, bytes...) //Ostali podaci

	return returnBytes
}

// Odpakuje sa zapisa na disku u podatak
func ByteToData(file *os.File, Offset... uint64) (string, *Data) {
	if len(Offset) > 0 {
		file.Seek(int64(Offset[0]), 0)
	}

	entry := ReadEntry(file)

	//Tombstone
	tombstone := false
	if entry.Tombstone[0] == byte(uint8(1)) {
		tombstone = true
	}
	timestamp := binary.BigEndian.Uint64(entry.Timestamp)
	data := NewData(entry.Value, tombstone, timestamp)
	Key := string(entry.Key)

	
	return Key, data
}

// Priprema summary u niz bajtova za upis
func summaryToByte(summary *Summary) []byte {
	firstKeyLen := len([]byte(summary.FirstKey))
	lastKeyLen := len([]byte(summary.LastKey))
	intervalsNum := len(summary.Intervals)

	//HEADER --> velicina prvog elementa, velicina drugog elementa, broj intervala
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(firstKeyLen))
	bytesLastKeyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesLastKeyLen, uint32(lastKeyLen))
	bytes = append(bytes, bytesLastKeyLen...)
	bytesIntervalsNum := make([]byte,4)
	binary.BigEndian.PutUint32(bytesIntervalsNum, uint32(intervalsNum))
	bytes = append(bytes, bytesIntervalsNum...)

	//GLAVNI DEO
	bytes = append(bytes, []byte(summary.FirstKey)...)
	bytes = append(bytes, []byte(summary.LastKey)...)

	//TABELA INTERVALA
	for i := 0; i < len(summary.Intervals); i++ {
		bytes = append(bytes, indexToByte(summary.Intervals[i])...)
	}

	return bytes
}

// Cita summary iz summary fajla
func byteToSummary(file *os.File) *Summary {
	summary := new(Summary)
	summary.Intervals = make([]*Index, 0)
	bytes := make([]byte, 4)

	//CITAMO HEADER
	//1 - duzina prvog kljuca
	//2 - duzina drugog kljuca
	//3 - broj intervala
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

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	intervalsNum := binary.BigEndian.Uint32(bytes)

	//CITAMO GLAVNI DEO
	bytes = make([]byte, firstKeyLen)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summary.FirstKey = string(bytes)

	bytes = make([]byte, lastKeyLen)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	summary.LastKey = string(bytes)

	//CITAMO NIZ INDEKSA
	index := new(Index)
	for i:=0; i < int(intervalsNum); i++ {
		index = byteToIndex(file)
		if index == nil {
			break
		}
		summary.Intervals = append(summary.Intervals, index)
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
	blm.Bitset = blm.Bitset[0:blm.M] //Osisamo visak u poslednjem bajtu

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


func CountMinSkechToBytes(cms *CountMinSketch) []byte {

	//upisujemo promenljive tipa uint32
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(cms.K))

	bytesM := make([]byte, 4)
	binary.BigEndian.PutUint32(bytesM, uint32(cms.M))
	bytes = append(bytes, bytesM...)

	//upisujemo promenljive tipa float64
	bitsEpsilon := math.Float64bits(cms.Epsilon)
	bytesEpsilon := make([]byte, 8)
	binary.BigEndian.PutUint64(bytesEpsilon, bitsEpsilon)
	bytes = append(bytes, bytesEpsilon...)

	bitsDelta := math.Float64bits(cms.Epsilon)
	bytesDelta := make([]byte, 8)
	binary.BigEndian.PutUint64(bytesDelta, bitsDelta)
	bytes = append(bytes, bytesDelta...)

	//upisujemo podatak po podatak iz valuetable-a
	for i:= 0; i < int(cms.K); i++ {
		for j:= 0; j < int(cms.M); j++ {
			bytesElem := make([]byte, 4)
			binary.BigEndian.PutUint32(bytesElem, uint32(cms.ValueTable[i][j]))
			bytes = append(bytes, bytesElem...)
		}
	}

	for _, fn := range cms.HashFuncs {
		//Belezimo duzinu svake hashfunkcije
		bytesHFLen := make([]byte, 4)
		binary.BigEndian.PutUint32(bytesHFLen, uint32(len(fn.Seed)))
		bytes = append(bytes, bytesHFLen...)

		//zapisuje hashfunkciju
		bytes = append(bytes, fn.Seed...)
	}
	return bytes
}

func BytesToCountMinSketch(file *os.File) *CountMinSketch {
	cms := new(CountMinSketch)
	bytes := make([]byte, 4)

	//Ucitavamo podatke tipa uint32
	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	cms.K = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	cms.M = binary.BigEndian.Uint32(bytes)

	//ucitavamo podatke tipa float64
	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsEpsilon := binary.BigEndian.Uint64(bytes)
	floatEpsilon := math.Float64frombits(bitsEpsilon)
	cms.Epsilon = floatEpsilon

	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	bitsDelta := binary.BigEndian.Uint64(bytes)
	floatDelta := math.Float64frombits(bitsDelta)
	cms.Delta = floatDelta

	//kreiramo ValueTable
	cms.ValueTable = make([][]uint32, cms.K)
	for i := range cms.ValueTable {
		cms.ValueTable[i] = make([]uint32, cms.M)
	}

	for i:= 0; i < int(cms.K); i++ {
		for j:= 0; j < int(cms.M); j++ {
			bytes := make([]byte, 4)
			_, err = file.Read(bytes)
			if err != nil {
				log.Fatal(err)
			}
			cms.ValueTable[i][j] = binary.BigEndian.Uint32(bytes)
		}
	}

	cms.HashFuncs = make([]CmsHashWithSeed, 0)
	hashWithSeed := new(CmsHashWithSeed)
	//Ucitavamo svaku hashfunkciju
	for i := uint32(0); i < cms.K; i++ {
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
		cms.HashFuncs = append(cms.HashFuncs, *hashWithSeed)
	}
	return cms
}

func HyperLogLogToBytes(hll *HLL) []byte {

	//upisujemo promenljive tipa uint32
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, hll.M)

	bytesP := make([]byte, 1)
	bytesP = append(bytesP, byte(hll.P))
	bytes = append(bytes, bytesP...)

	bytesReg := make([]byte, hll.M)
	for b:= range hll.Reg {
		bytesReg = append(bytesReg, byte(b))
	}
	bytes = append(bytes, bytesReg...)

	return bytes
}

func BytesToHyperLogLog(file *os.File) *HLL {
	hll := new(HLL)
	bytes := make([]byte, 8)

	//ucitavamo podatke
	_, err := file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	hll.M = binary.BigEndian.Uint64(bytes)

	bytes = make([]byte, 1)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	hll.P = uint8(bytes[0])

	hll.Reg = make([]uint8, hll.M)
	for i:= uint64(0); i < hll.M; i++ {
		bytes = make([]byte, 1)
		_, err := file.Read(bytes)
		if err != nil {
			log.Fatal(err)
		}
		hll.Reg = append(hll.Reg, bytes...)
	}

	return hll
}

