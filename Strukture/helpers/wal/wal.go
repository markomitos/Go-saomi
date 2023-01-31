package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
	. "project/gosaomi/memtable"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a Value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WriteAheadLog struct {
	buffer          []byte
	buffer_capacity uint
	buffer_size     uint
	directory       string
	current_offset  uint
	low_water_mark  uint
}

// struktura za svaki pojedinac zapis
type Entry struct {
	Crc        []byte
	Timestamp  []byte
	Tombstone  []byte
	Key_size   []byte
	Value_size []byte
	Key        []byte
	Value      []byte
}

// Konstruktor jednog unosa
func (wal *WriteAheadLog) NewEntry(key string, data *Data) *Entry {
	e := new(Entry)

	keyBytes := []byte(key)

	//izracunaj duzinu kljuca i vrednosti
	e.Key_size = make([]byte, 8)
	e.Value_size = make([]byte, 8)
	binary.BigEndian.PutUint64(e.Key_size, uint64(int64(len(keyBytes))))
	binary.BigEndian.PutUint64(e.Value_size, uint64(int64(len(data.Value))))

	e.Key = keyBytes
	e.Value = data.Value
	e.Timestamp = make([]byte, 8)
	binary.BigEndian.PutUint64(e.Timestamp, data.Timestamp)

	tombstoneBytes := make([]byte, 0)
	if data.Tombstone {
		tombstoneBytes = append(tombstoneBytes, uint8(1))
	} else {
		tombstoneBytes = append(tombstoneBytes, uint8(0))
	}
	e.Tombstone = tombstoneBytes

	//ubaci sve u niz bajtova da bi napravio Crc
	bytes := make([]byte, 0)
	bytes = append(bytes, e.Timestamp...)
	bytes = append(bytes, e.Tombstone...)
	bytes = append(bytes, e.Key_size...)
	bytes = append(bytes, e.Value_size...)
	bytes = append(bytes, e.Key...)
	bytes = append(bytes, e.Value...)
	e.Crc = make([]byte, 4)
	binary.BigEndian.PutUint32(e.Crc, uint32(CRC32(bytes)))
	return e
}

// pretvara iz Entry u niz bitova da bi mogli da zapisemo u fajlu
func EntryToBytes(e *Entry) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, e.Crc...)
	bytes = append(bytes, e.Timestamp...)
	bytes = append(bytes, e.Tombstone...)
	bytes = append(bytes, e.Key_size...)
	bytes = append(bytes, e.Value_size...)
	bytes = append(bytes, e.Key...)
	bytes = append(bytes, e.Value...)
	return bytes
}

// pretvara iz niza bytova u Entry da bi mogli da procitamo vrednosti iz fajla
func BytesToEntry(bytes []byte) *Entry {
	e := new(Entry)
	e.Crc = bytes[CRC_START:TIMESTAMP_START]
	e.Timestamp = bytes[TIMESTAMP_START:TOMBSTONE_START]
	e.Tombstone = bytes[TOMBSTONE_START:KEY_SIZE_START]
	e.Key_size = bytes[KEY_SIZE_START:VALUE_SIZE_START]
	e.Value_size = bytes[VALUE_SIZE_START:KEY_START]
	e.Key = bytes[KEY_START : KEY_START+binary.BigEndian.Uint64(e.Key_size)]
	e.Value = bytes[KEY_START+binary.BigEndian.Uint64(e.Key_size) : KEY_START+binary.BigEndian.Uint64(e.Key_size)+binary.BigEndian.Uint64(e.Value_size)]
	return e
}

// inicijalizuje Write Ahead Log i ukoliko logovi vec postoje povecava offset do posle poslednjeg loga
func NewWriteAheadLog(directory string) *WriteAheadLog {
	config := GetConfig()

	//ukoliko ne postoji napravi direktorijum
	_, err := os.Stat(directory)
	if os.IsNotExist(err) {
		err = os.MkdirAll(directory, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}
	wal := new(WriteAheadLog)
	wal.directory = directory
	wal.current_offset = 0
	//ukoliko postoji vec direktorijum sa logovima azuriramo offset
	for {
		filename := wal.generateSegmentFilename()
		_, err := os.Stat(filename)
		if os.IsNotExist(err) {
			break
		}
		wal.current_offset++
	}

	//zadajemo inicijalne vrednosti
	wal.buffer = make([]byte, 0)
	wal.low_water_mark = config.WalWaterMark
	wal.buffer_capacity = config.WalBufferCapacity
	wal.buffer_size = 0
	return wal

}

// generise ime filea sa trenutnim offsetom
func (wal *WriteAheadLog) generateSegmentFilename(offset ...uint) string {
	chosen_offset := wal.current_offset
	if len(offset) > 0 {
		chosen_offset = offset[0]
	}
	filename := wal.directory + "/wal_"
	ustr := strconv.FormatUint(uint64(chosen_offset), 10)

	//upotpunjava ime sa potrebnim nizom nula ukoliko ofset nije vec petocifren broj
	for len(ustr) < 5 {
		ustr = "0" + ustr
	}
	filename += ustr + ".log"
	return filename
}

// kreira file sa narednim offsetom
func (wal *WriteAheadLog) NewWALFile() *os.File {

	filename := wal.generateSegmentFilename()
	wal.current_offset++
	//pravi file u wal direktorijumu
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

// brise sve osim poslednjeg segmenta
func (wal *WriteAheadLog) deleteOldSegments() {
	wal.current_offset--
	for offset := uint(0); offset < wal.current_offset; offset++ {
		err := os.Remove(wal.generateSegmentFilename(offset))
		if err != nil {
			log.Fatal(err)
		}
	}
	//preimenuje poslednji log u prvi i vraca offset na svoje mesto
	err := os.Rename(wal.generateSegmentFilename(wal.current_offset), wal.generateSegmentFilename(0))
	if err != nil {
		log.Fatal(err)
	}
	wal.current_offset = 1
}

// batch zapis - zapisuje ceo buffer u segment sa sledecim offsetom
func (wal *WriteAheadLog) WriteBuffer() {
	//kreira fajl sa narednim offsetom
	file := wal.NewWALFile()

	//zapisujemo ceo buffer u novi fajl
	_, err := file.Write(wal.buffer)
	if err != nil {
		log.Fatal(err)
	}
	wal.buffer = make([]byte, 0)
	wal.buffer_size = 0
	file.Close()
}

// dodajemo entry u baffer, ukoliko je pun zapisuje buffer u segment
func (wal *WriteAheadLog) addEntryToBuffer(entry *Entry) {
	wal.buffer = append(wal.buffer, EntryToBytes(entry)...)
	wal.buffer_size++
	if wal.buffer_size == wal.buffer_capacity {
		wal.WriteBuffer()
		if wal.current_offset > wal.low_water_mark {
			wal.deleteOldSegments()
		}
	}

}

// zapisuje direktno entry
func (wal *WriteAheadLog) WriteEntry(entry *Entry) {
	//otvaramo file u append only rezimu
	wal.current_offset--
	filename := wal.generateSegmentFilename()
	wal.current_offset++
	file, err := os.OpenFile(filename, os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	//zapisujemo entry kao niz bytova
	_, err = file.Write(EntryToBytes(entry))
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
}

// cita niz bitova i pretvara ih u klasu entity za dalju obradu
func ReadEntry(file *os.File) *Entry {
	//prvo procitamo do kljuca da bi videli koje su  velicine kljuc i vrednost
	bytes := make([]byte, KEY_START)
	_, err := file.Read(bytes)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Fatal(err)
	}
	//procitamo velicine kljuca i vrednosti
	Key_size := bytes[KEY_SIZE_START:VALUE_SIZE_START]
	Value_size := bytes[VALUE_SIZE_START:]
	//procitamo kljuc
	Key := make([]byte, int(binary.BigEndian.Uint64(Key_size)))
	_, err = file.Read(Key)
	if err != nil {
		log.Fatal(err)
	}

	//procitamo vrednost
	Value := make([]byte, int(binary.BigEndian.Uint64(Value_size)))
	_, err = file.Read(Value)
	if err != nil {
		log.Fatal(err)
	}

	bytes = append(bytes, Key...)
	bytes = append(bytes, Value...)

	entry := BytesToEntry(bytes)

	return entry
}

// ispis pojedinacnog unosa
func (entry *Entry) Print() {
	Timestamp := binary.BigEndian.Uint64(entry.Timestamp)
	Key_size := binary.BigEndian.Uint64(entry.Key_size)
	Value_size := binary.BigEndian.Uint64(entry.Value_size)
	//Tombstone
	tombstone := false
	if entry.Tombstone[0] == byte(uint8(1)) {
		tombstone = true
	}
	println("Entry: ")
	println("CRC: ", entry.Crc)
	println("Timestamp: ", Timestamp)
	println("Tombstone: ", tombstone)
	println("Key size: ", Key_size)
	println("Value size: ", Value_size)
	println("Key: ", string(entry.Key))
	println("Value: ", string(entry.Value))
	println("---------------------------------------")
}

// cita pojedinacan segment
func (wal *WriteAheadLog) readLog(filename string) {
	file, err := os.Open(filename)
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

// cita hronoloskim redom sve segmente
func (wal *WriteAheadLog) ReadAllLogs() {
	offset := uint(0)
	for offset < wal.current_offset {
		println("==========================================================")
		println("Current offset: ", offset)
		println("==========================================================")
		wal.readLog(wal.generateSegmentFilename(offset))
		offset++
	}
}

func (wal *WriteAheadLog) InitiateMemTable() MemTable {
	config := GetConfig()
	memTable := NewMemTable(config.MemtableSize)
	file, err := os.Open(wal.generateSegmentFilename())
	if err != nil {
		log.Fatal(err)
	}

	for {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}
		tombstone := false
		if entry.Tombstone[0] == byte(uint8(1)) {
			tombstone = true
		}
		timestamp := binary.BigEndian.Uint64(entry.Timestamp)
		data := NewData(entry.Value, tombstone, timestamp)
		key := string(entry.Value)
		memTable.Put(key, data)
	}
	file.Close()
	return memTable
}

func main() {
	wal := NewWriteAheadLog("test")
	data := new(Data)
	data.Value = []byte("majmun")
	data.Timestamp = uint64(time.Now().Unix())
	data.Tombstone = true
	for i := 0; i < 101; i++ {
		e := wal.NewEntry(strconv.Itoa(i*125), data)
		e.Print()
	}
	wal.ReadAllLogs()
}
