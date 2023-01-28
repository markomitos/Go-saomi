package wal

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
	low_water_mark  uint64
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

// Konstruktor jednog unosa i automatski taj unos ubacuje u buffer
func (wal *WriteAheadLog) NewEntry(key []byte, value []byte) *Entry {
	e := new(Entry)

	//izracunaj duzinu kljuca i vrednosti
	e.key_size = make([]byte, 8)
	e.value_size = make([]byte, 8)
	binary.BigEndian.PutUint64(e.key_size, uint64(int64(len(key))))
	binary.BigEndian.PutUint64(e.value_size, uint64(int64(len(value))))

	e.key = key
	e.value = value
	e.timestamp = make([]byte, 8)
	binary.BigEndian.PutUint64(e.timestamp, uint64(time.Now().Unix()))
	e.tombstone = make([]byte, 1)

	//ubaci sve u niz bajtova da bi napravio crc
	bytes := make([]byte, 0)
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
	bytes := make([]byte, 0)
	bytes = append(bytes, e.crc...)
	bytes = append(bytes, e.timestamp...)
	bytes = append(bytes, e.tombstone...)
	bytes = append(bytes, e.key_size...)
	bytes = append(bytes, e.value_size...)
	bytes = append(bytes, e.key...)
	bytes = append(bytes, e.value...)
	return bytes
}

// pretvara iz niza bytova u Entry da bi mogli da procitamo vrednosti iz fajla
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

// inicijalizuje Write Ahead Log i ukoliko logovi vec postoje povecava offset do posle poslednjeg loga
func NewWriteAheadLog(directory string) *WriteAheadLog {
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
	wal.low_water_mark = 100
	wal.buffer_capacity = 100
	wal.buffer_size = 0
	return wal

}

// generise ime filea sa trenutnim offsetom
func (wal *WriteAheadLog) generateSegmentFilename(offset ...uint64) string {
	chosen_offset := wal.current_offset
	if len(offset) > 0 {
		chosen_offset = offset[0]
	}
	filename := wal.directory + "/wal_"
	ustr := strconv.FormatUint(chosen_offset, 10)

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

// brise sve osim poslednjeg segmenta
func (wal *WriteAheadLog) deleteOldSegments() {
	wal.current_offset--
	for offset := uint64(0); offset < wal.current_offset; offset++ {
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
	file := wal.newWALFile()

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
	wal.buffer = append(wal.buffer, entryToBytes(entry)...)
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
	_, err = file.Write(entryToBytes(entry))
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
}

// cita niz bitova i pretvara ih u klasu entity za dalju obradu
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
	key := make([]byte, int(binary.BigEndian.Uint64(key_size)))
	_, err = file.Read(key)
	if err != nil {
		log.Fatal(err)
	}

	//procitamo vrednost
	value := make([]byte, int(binary.BigEndian.Uint64(value_size)))
	_, err = file.Read(value)
	if err != nil {
		log.Fatal(err)
	}

	bytes = append(bytes, key...)
	bytes = append(bytes, value...)

	entry := bytesToEntry(bytes)

	return entry
}

// ispis pojedinacnog unosa
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

// cita pojedinacan segment
func (wal *WriteAheadLog) readLog(filename string) {
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
	file.Close()

}

// cita hronoloskim redom sve segmente
func (wal *WriteAheadLog) ReadAllLogs() {
	offset := uint64(0)
	for offset < wal.current_offset {
		println("==========================================================")
		println("Current offset: ", offset)
		println("==========================================================")
		wal.readLog(wal.generateSegmentFilename(offset))
		offset++
	}
}

func main() {
	wal := NewWriteAheadLog("test")
	for i := 0; i < 101; i++ {
		e := wal.NewEntry([]byte(strconv.Itoa(i*125)), []byte("mjm"))
		e.print()
	}
	entry := wal.NewEntry([]byte("789"), []byte("majmun"))
	entry.key = []byte("456")
	entry.value = []byte("mijmun")

	wal.WriteEntry(entry)
	wal.ReadAllLogs()
}
