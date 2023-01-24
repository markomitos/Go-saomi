package main

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
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
	current_offset  uint64
}

// struktura za svaki pojedinac zapis
type Entry struct {
	crc        []byte
	timestamp  []byte
	tombstone  []byte
	key_size   []byte
	value_size []byte
	key        []byte
	value      []byte
}

// Konstruktor
func (wal *WriteAheadLog) newEntry(key []byte, value []byte) *Entry {
	e := new(Entry)
	e.key_size = make([]byte, 8)
	e.value_size = make([]byte, 8)
	binary.BigEndian.PutUint64(e.key_size, uint64(int64(len(key))))
	binary.BigEndian.PutUint64(e.value_size, uint64(int64(len(value))))
	e.key = key
	e.value = value
	e.timestamp = make([]byte, 8)
	binary.BigEndian.PutUint64(e.timestamp, uint64(time.Now().Unix()))
	e.tombstone = make([]byte, 1)
	bytes := make([]byte, 25+len(key)+len(value))
	bytes = append(bytes, e.timestamp...)
	bytes = append(bytes, e.tombstone...)
	bytes = append(bytes, e.key_size...)
	bytes = append(bytes, e.value_size...)
	bytes = append(bytes, e.key...)
	bytes = append(bytes, e.value...)
	e.crc = make([]byte, 4)
	binary.BigEndian.PutUint32(e.crc, uint32(CRC32(bytes)))
	wal.addEntryToBuffer(e)
	return e
}

// pretvara iz Entry u niz bitova da bi mogli da zapisemo u fajlu
func entryToBytes(e *Entry) []byte {
	bytes := make([]byte, 29+binary.BigEndian.Uint64(e.key_size)+binary.BigEndian.Uint64(e.value_size))
	bytes = append(bytes, e.timestamp...)
	bytes = append(bytes, e.tombstone...)
	bytes = append(bytes, e.key_size...)
	bytes = append(bytes, e.value_size...)
	bytes = append(bytes, e.key...)
	bytes = append(bytes, e.value...)
	return bytes
}

// pretvara iz niza bytova u ENtry da bi mogli da procitamo vrednosti iz fajla
func bytesToEntry(bytes []byte) *Entry {
	e := new(Entry)
	e.crc = bytes[CRC_START:TIMESTAMP_START]
	e.timestamp = bytes[TIMESTAMP_START:TOMBSTONE_START]
	e.tombstone = bytes[TOMBSTONE_START:KEY_SIZE_START]
	e.key_size = bytes[KEY_SIZE_START:VALUE_SIZE_START]
	e.value_size = bytes[VALUE_SIZE_START:KEY_START]
	e.key = bytes[KEY_START : KEY_START+binary.BigEndian.Uint64(e.key_size)]
	e.value = bytes[KEY_START+binary.BigEndian.Uint64(e.key_size) : KEY_START+binary.BigEndian.Uint64(e.key_size)+binary.BigEndian.Uint64(e.value_size)]
	return e
}

func newWriteAheadLog(directory string) *WriteAheadLog {
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

	wal.buffer_capacity = 10
	wal.buffer_size = 0
	return wal

}

// generise ime filea sa trenutnim offsetom
func (wal *WriteAheadLog) generateSegmentFilename() string {
	filename := wal.directory + "/wal_"
	ustr := strconv.FormatUint(wal.current_offset, 10)

	//upotpunjava ime sa potrebnim nizom nula ukoliko ofset nije vec petocifren broj
	for len(ustr) < 5 {
		ustr = "0" + ustr
	}
	filename += ustr + ".log"
	return filename
}

// kreira file sa narednim offsetom
func (wal *WriteAheadLog) newWALFile() *os.File {

	filename := wal.generateSegmentFilename()
	wal.current_offset++
	//pravi file u wal direktorijumu
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

// batch zapis
func (wal *WriteAheadLog) writeBuffer() {
	//kreira fajl sa narednim offsetom
	file := wal.newWALFile()

	//zapisujemo ceo buffer u novi fajl
	_, err := file.Write(wal.buffer)
	if err != nil {
		log.Fatal(err)
	}
	wal.buffer = make([]byte, 0)
	wal.buffer_size = 0
}

func (wal *WriteAheadLog) addEntryToBuffer(entry *Entry) {
	wal.buffer = append(wal.buffer, entryToBytes(entry)...)
	wal.buffer_size++
	if wal.buffer_size == wal.buffer_capacity {
		wal.writeBuffer()
	}
}

// zapisuje direktno entry
func (wal *WriteAheadLog) writeEntry(entry *Entry) {
	//otvaramo file u append only rezimu
	filename := wal.generateSegmentFilename()
	file, err := os.OpenFile(filename, os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	//zapisujemo entry kao niz bytova
	_, err = file.Write(entryToBytes(entry))
	if err != nil {
		log.Fatal(err)
	}
}

// TO DO
func readEntry(file *os.File) *Entry {
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
	key_size := bytes[KEY_SIZE_START:VALUE_SIZE_START]
	value_size := bytes[VALUE_SIZE_START:]

	//procitamo kljuc
	key := make([]byte, binary.BigEndian.Uint64(key_size))
	_, err = file.Read(key)
	if err != nil {
		log.Fatal(err)
	}

	//procitamo vrednost
	value := make([]byte, binary.BigEndian.Uint64(value_size))
	_, err = file.Read(value)
	if err != nil {
		log.Fatal(err)
	}

	bytes = append(bytes, key...)
	bytes = append(bytes, value...)

	entry := bytesToEntry(bytes)

	return entry
}

func (entry *Entry) print() {
	timestamp := binary.BigEndian.Uint64(entry.timestamp)
	key_size := binary.BigEndian.Uint64(entry.key_size)
	value_size := binary.BigEndian.Uint64(entry.value_size)
	println("Entry: ")
	println("CRC: ", entry.crc)
	println("Timestamp: ", timestamp)
	println("Tombstone: ", entry.tombstone)
	println("Key size: ", key_size)
	println("Value size: ", value_size)
	println("Key: ", string(entry.key))
	println("Value: ", string(entry.value))
	println("---------------------------------------")
}

func (wal *WriteAheadLog) readAllEntries(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	for {
		entry := readEntry(file)
		if entry == nil {
			break
		}
		entry.print()
	}

}

func main() {
	wal := newWriteAheadLog("test")
	for i := 0; i < 11; i++ {
		e := wal.newEntry([]byte(strconv.Itoa(i)), []byte("majmun"))
		e.print()
	}
	wal.readAllEntries("test/wal_00000.log")
}
