package lsm

import (
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
	. "project/gosaomi/sstable"
	"strconv"
)

//Ovde organizujemo fajlove pri upisu


type Lsm struct{
	MaxLevel uint32
	Level uint32 //Trenutna visina
	LevelSizes []uint32  //cuva broj sstabela u svakom nivou
}

func InitializeLsm(){
	_, err := os.Stat("files/sstable/lsm.bin")
	if os.IsNotExist(err){
		config := GetConfig()
		lsm := new(Lsm)
		lsm.MaxLevel = uint32(config.LsmMaxLevel)
		lsm.Level = 1
		lsm.LevelSizes = make([]uint32, lsm.MaxLevel)

		path, err := filepath.Abs("files/sstable")
		if err != nil{
			log.Fatal(err)
		}
		file, err := os.Create(path+"/lsm.bin")
		if err != nil {
			log.Fatal(err)
		}
		file.Close()

		lsm.Write()
		lsm.GenerateLevelFolders()
	} else {
		//Ukoliko je maxlevel veci od broja trenutnih foldera kreirace se novi
		ReadLsm().GenerateLevelFolders()
	}

	
}

//Zapisuje lsm u fajl
func (lsm *Lsm) Write(){
	filePath, err1 := filepath.Abs("files/sstable/lsm.bin")
	if err1 != nil{
		log.Fatal(err1)
	}
	file, err := os.OpenFile(filePath, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}

	bytes := make([]byte,4)
	binary.BigEndian.PutUint32(bytes, lsm.MaxLevel)

	tempBytes := make([]byte,4)
	binary.BigEndian.PutUint32(tempBytes, lsm.Level)
	bytes = append(bytes, tempBytes...)

	for i:=0; i < len(lsm.LevelSizes); i++{
		tempBytes = make([]byte,4)
		binary.BigEndian.PutUint32(tempBytes, lsm.LevelSizes[i])
		bytes = append(bytes, tempBytes...)
	}

	_, err = file.Write(bytes)
	if err != nil {
		log.Fatal(err)
	}

	file.Close()
}

func ReadLsm() *Lsm{
	filePath, err1 := filepath.Abs("files/sstable/lsm.bin")
	if err1 != nil{
		log.Fatal(err1)
	}
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	lsm := new(Lsm)

	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	lsm.MaxLevel = binary.BigEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}
	lsm.Level = binary.BigEndian.Uint32(bytes)

	for true{
		bytes = make([]byte, 4)
		_, err = file.Read(bytes)
		if err != nil {
			if err == io.EOF{
				break
			}
			log.Fatal(err)
		}
		lsm.LevelSizes = append(lsm.LevelSizes, binary.BigEndian.Uint32(bytes))
	}

	file.Close()
	return lsm
}

//Imenuje sstabelu nakon flusha
func GenerateFlushName() string{
	lsm := ReadLsm()
	currentMax := lsm.LevelSizes[0]
	return "level1/sstable" + strconv.FormatUint(uint64(currentMax + 1), 10)
}

func (lsm *Lsm) GenerateSSTableName(currentLevel uint32, index uint32) string {
	return "level"+ strconv.FormatUint(uint64(currentLevel), 10) + "/sstable" + strconv.FormatUint(uint64(index), 10)
}

//Pokrece se pri upisu nove sstabele
//Povecava trenutni broj za 1 u levelu
func IncreaseLsmLevel(level uint32){
	lsm := ReadLsm()
	lsm.LevelSizes[level-1]++
	lsm.Write()
}

//Menja imena fajlova tako da krecu od 1
//Update-a velicinu levela
func RenameLevel(level uint32){
	lsm := ReadLsm()
	if(lsm.LevelSizes[level-1] % 2 != 0){
		err := os.Rename("sstable"+strconv.FormatUint(uint64(lsm.LevelSizes[level-1]), 10), "sstable1")
		if err != nil {
			log.Fatal(err)
		}
		lsm.LevelSizes[level-1] = 1
	} else {
		lsm.LevelSizes[level-1] = 0
	}
	lsm.Write()
}

//Funkcija koja generise foldere do max nivoa
func (lsm *Lsm) GenerateLevelFolders() {
	path, err := filepath.Abs("files/sstable")
	if err != nil{
		log.Fatal(err)
	}

	for i:=uint32(1); i < lsm.MaxLevel+1; i++{
		_, err := os.Stat(path+"/level"+strconv.FormatUint(uint64(i), 10))
		if os.IsNotExist(err) {
			err = os.Mkdir(path+"/level"+strconv.FormatUint(uint64(i), 10), os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

//Poziva se u mainu i pokrece izabranu kompakciju
func RunCompact(){
	lsm := ReadLsm()
	//Iteriramo po levelima
	//Preskacemo poslednji level jer se tu ne radi kompakcija
	for i:=uint32(1); i < lsm.MaxLevel-1; i++{
		if lsm.LevelSizes[i-1] >= 2{
			lsm.Compact(i)
		}
	}

	//Iteriramo po levelima i preimenujemo fajlove ukoliko je potrebno
	for i:=uint32(1); i < lsm.MaxLevel-1; i++{
		RenameLevel(i)
	}
}

func (lsm *Lsm) Compact(currentLevel uint32){
	config := GetConfig()
	if config.CompactionType == "size_tiered" {
		lsm.SizeTieredCompaction(currentLevel)
	} else if config.CompactionType == "leveled" {
		lsm.LeveledCompaction(currentLevel)
	}
}

func (lsm *Lsm) SizeTieredCompaction(currentLevel uint32){
	config := GetConfig()

	//Racunamo velicinu naredne sstabele kao duplu od prethodne
	//jer ne znamo kolika ce tacno biti velicina,
	//mada ona nije ni toliko bitna jer je koristi samo bloomfilter za inicijalizaciju
	//On prima ocekivani broj elemenata tako da ovo nece biti greska
	size := uint32(math.Pow(2, float64(currentLevel-1)) * float64(config.MemtableSize))

	//Uzimamo po 2 sstabele i radimo kompakciju nad njima
	for index := uint32(0); index < lsm.LevelSizes[currentLevel-1]; index += 2{
		
		firstSStable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, index))
		secondSStable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, index+1))

		mergedKeys, mergedData := MergeSSTables(firstSStable, secondSStable)

		lsm.LevelSizes[currentLevel]++
		mergedSSTable := NewSSTable(size*2,lsm.GenerateSSTableName(currentLevel+1, lsm.LevelSizes[currentLevel]))
		mergedSSTable.Flush(mergedKeys, mergedData)

		//Brisemo stare sstabele
		deleteSSTable(lsm.GenerateSSTableName(currentLevel, index))
		deleteSSTable(lsm.GenerateSSTableName(currentLevel, index+1))
		
	}
	lsm.Write()
}


//TO DO
func (lsm *Lsm) LeveledCompaction(currentLevel uint32){
	
}

func MergeSSTables(firstSStable SST,secondSStable SST) ([]string, []*Data){
	file1, data1End  := firstSStable.GoToData()
	file2, data2End  := secondSStable.GoToData()
	defer file1.Close()
	defer file2.Close()

	mergedKeys := make([]string,0)
	mergedData := make([]*Data,0)

	key1, data1 := "", new(Data)
	key2, data2 := "", new(Data)

	//Pomocne promenljive koje nam govore da li treba ici na sledeci element
	toRead1 := true
	toRead2 := true
	for true {
		//Kraj prve tabele
		if isEndOfData(file1, data1End){
			if isEndOfData(file2, data2End){
				break
			}

			if key1 == key2{
				key2, data2 = ByteToData(file2)
			}

			//Prolazimo samo kroz drugu tabelu da prebacimo ostatak
			for true{
				if !data2.Tombstone{
					mergedKeys = append(mergedKeys, key2)
					mergedData = append(mergedData, data2)
				}
				if isEndOfData(file2, data2End){
					break
				}
				key2, data2 = ByteToData(file2)
			}
			break
		}

		//Kraj druge tabele
		if isEndOfData(file2, data2End){
			//Ukoliko su bili jednaki moramo preskociti trenutan
			if key1 == key2{
				key1, data1 = ByteToData(file1)
			}

			//Prolazimo samo kroz prvu tabelu da prebacimo ostatak
			for true{
				if !data1.Tombstone{
					mergedKeys = append(mergedKeys, key1)
					mergedData = append(mergedData, data1)
				}
				if isEndOfData(file1, data1End){
					break
				}
				key1, data1 = ByteToData(file1)
			}
			break
			
		}

		if toRead1{
			key1, data1 = ByteToData(file1)
		}
		if toRead2{
			key2, data2 = ByteToData(file2)
		}

		if key1 == key2 {
			if data2.Timestamp >= data1.Timestamp{
				if !data2.Tombstone{
					mergedKeys = append(mergedKeys, key2)
					mergedData = append(mergedData, data2)
				}
			} else {
				if !data1.Tombstone{
					mergedKeys = append(mergedKeys, key1)
					mergedData = append(mergedData, data1)
				}
			}
			toRead1 = true
			toRead2 = true
		} else if(key1 < key2){
			if !data1.Tombstone{
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
			}
			toRead1 = true
			toRead2 = false
		} else if(key2 < key1){
			if !data2.Tombstone{
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
			}
			toRead1 = false
			toRead2 = true
		}

	}
	return mergedKeys, mergedData
}

func isEndOfData(file *os.File, dataEnd uint64) bool{
	//Proveravamo da li smo prosli data zonu
	currentOffset, err := file.Seek(0,1)
	if err != nil{
		log.Fatal(err)
	}
	if uint64(currentOffset) >= dataEnd {
		return true
	}
	return false
}

func deleteSSTable(directory string){
	err := os.Remove("files/sstable"+directory)
	if err != nil{
		log.Fatal(err)
	}
}