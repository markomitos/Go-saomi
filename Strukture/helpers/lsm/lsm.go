package lsm

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	. "project/gosaomi/config"
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

//Pokrece se pri upisu nove sstabele
//Povecava trenutni broj za 1 u levelu
func IncreaseLsmLevel(level uint32){
	lsm := ReadLsm()
	lsm.LevelSizes[level-1]++
	lsm.Write()
}

//Menja imena fajlova tako da krecu od 1
//Update-a velicinu levela
func UpdateLevel(level uint32){
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

