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
	. "project/gosaomi/scan"
	. "project/gosaomi/sstable"
	"strconv"
)

//Ovde organizujemo fajlove pri upisu


type Lsm struct{
	MaxLevel uint32
	Level uint32 //Trenutna visina
	LevelSizes []uint32  //cuva broj sstabela u svakom nivou
}

//Kreira foldere i lsm fajl ako ne postoji
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

//Ucitava LSM sa diska
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

		err := os.Rename("files/sstable/level"+strconv.FormatUint(uint64(level), 10)+"/sstable"+strconv.FormatUint(uint64(lsm.LevelSizes[level-1]), 10),
		"files/sstable/level"+strconv.FormatUint(uint64(level), 10) + "/sstable1")
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
	for i:=uint32(1); i < lsm.MaxLevel; i++{
		if lsm.LevelSizes[i-1] >= 2{
			lsm.Compact(i)
		}
	}

	//Iteriramo po levelima i preimenujemo fajlove ukoliko je potrebno
	for i:=uint32(1); i < lsm.MaxLevel; i++{
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
	size := getSSTableSize(currentLevel)

	//Uzimamo po 2 sstabele i radimo kompakciju nad njima
	for index := uint32(1); index < lsm.LevelSizes[currentLevel-1]; index += 2{
		
		firstSStable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, index))
		secondSStable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, index+1))

		mergedKeys, mergedData := Merge2SSTables(firstSStable, secondSStable)

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
	config := GetConfig()
	//Racuna broj sstabela koji je dozvoljen u trenutnom nivou
	maxSSTables := math.Pow(float64(config.LeveledCompactionMultiplier), float64(currentLevel))

	//Proveravamo da li je uopste potrebno raditi kompakciju na ovom nivou
	if lsm.LevelSizes[currentLevel-1] > uint32(maxSSTables){
		sstableArr := make([]SST, 0)
		if currentLevel == 1 {
			//TO DO: spajaj sve sstabele i pomeraj level gore
			for index := uint32(1); index < lsm.LevelSizes[currentLevel-1]; index++{
				sstableArr = append(sstableArr, NewSSTable(uint32(config.MemtableSize),lsm.GenerateSSTableName(currentLevel, index)))
			}
			MergeSSTables(sstableArr, currentLevel)
		} else {
			//prvi slucaj je da ispod nema poklapanja
			//drugi slucaj je da ispod ima poklapanja
		}
		//Ukoliko se nesto dodalo u naredni nivo proveravamo da li je i tamo potrebna kompakcija
		lsm.LeveledCompaction(currentLevel+1)
	}
}

//Spaja 2 sstabele
func Merge2SSTables(firstSStable SST,secondSStable SST) ([]string, []*Data){
	file1, data1End  := firstSStable.GoToData()
	file2, data2End  := secondSStable.GoToData()

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
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
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
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
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
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
			} else {
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
			}
			toRead1 = true
			toRead2 = true
		} else if(key1 < key2){
			mergedKeys = append(mergedKeys, key1)
			mergedData = append(mergedData, data1)
			toRead1 = true
			toRead2 = false
		} else if(key2 < key1){
			mergedKeys = append(mergedKeys, key2)
			mergedData = append(mergedData, data2)
			toRead1 = false
			toRead2 = true
		}

	}
	file1.Close()
	file2.Close()
	return mergedKeys, mergedData
}

func MergeSSTables(sstables []SST, currentLevel uint32) ([]string, []*Data){
	config := GetConfig()

	files := make([]*os.File,0) //Ovde cuvamo otvorene fajlove od svih sstabela
	dataEnds := make([]uint64, 0) //Ovde cuvamo krajeve data zona za svaku sstabelu
	sstableLevels := make([]uint32, 0) //Ovde cuvamo u kojem nivou se svaka sstabela nalazi
	sstableFileNumbers := make([]uint32, 0) //Ovde cuvamo koje po redu su sstabele
	isEndOfFiles := make([]bool, 0) //Ovde cuvamo bool vrednost da li je cela sstabela predjena

	keys := make([]string, 0) //Ovde cuvamo trenutne kljuceve
	data := make([]*Data, 0) //Ovde cuvamo trenutan podatak
	toRead := make([]bool, 0) //Flag da li je potrebno citanje sledeceg elementa

	//Identifikacija sstabela i dodavanje u niz
	for i:=0; i < len(sstables); i++{
		currentSSTable := sstables[i]
		levelNum, fileNum := currentSSTable.GetPosition()
		sstableLevels = append(sstableLevels, levelNum)
		sstableFileNumbers = append(sstableFileNumbers, fileNum)
		
		//otvaramo fajl i pozicioniramo se na data zonu
		file, dataEnd := currentSSTable.GoToData()
		files = append(files, file)
		dataEnds = append(dataEnds, dataEnd)

		isEndOfFiles = append(isEndOfFiles, false) //na pocetku inicijalizujemo na false

		toRead = append(toRead, true) //Uvek prve elemente citamo
	}

	mergedKeys := make([]string,0)
	mergedData := make([]*Data,0)

	for true {
		tempKeys := make([]string, 0)//Cuvamo kljuceve koje uporedjujemo (necemo cuvati kljuceve od fajlova koji su predjeni)
		tempData := make([]*Data, 0)//Cuvamo podatke koje uporedjujemo

		//Prolazimo kroz sve fajlove
		for i:=0; i < len(files); i++{
			if !isEndOfData(files[i], dataEnds[i]){
				if toRead[i]{
					keys[i], data[i] = ByteToData(files[i])
				}
				tempKeys = append(tempKeys, keys[i])
				tempData = append(tempData, data[i])
			} else {
				isEndOfFiles[i] = true
			}
		}

		//Svi su ubaceni i kraj
		if len(tempKeys) < 1{
			break
		}

		//Trazimo koji je element sa najmanjom vrednoscu
		minKey := tempKeys[0]
		for i:=1; i<len(tempKeys); i++{
			if tempKeys[i] < minKey{
				minKey = tempKeys[i]
			}
		}

		//Trazimo koji element treba da procitamo po najvecem timestampu
		newestData := new(Data)
		newestData.Timestamp = 0
		for i:=1; i<len(tempData); i++{
			if keys[i] == minKey{
				if tempData[i].Timestamp > newestData.Timestamp{
					newestData = tempData[i]
				}
			}
		}

		//Obelezavamo sve koji su jednaki izabranom kljucu da se citaju
		//Ostali zadrzavaju vrednost
		for i:=0; i < len(toRead); i++{
			toRead[i] = keys[i] == minKey //Ovo neces videti u Novom Sadu :)
		}

		//Dodajemo u red za upis u novu sstabelu
		mergedKeys = append(mergedKeys, minKey)
		mergedData = append(mergedData, newestData)

		//Proveravamo da li smo napunili sstabelu
		//Ukoliko jesmo flushujemo u visi nivo
		if len(mergedKeys) >= int(config.MemtableSize){
			
		}
	}



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
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
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
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
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
				mergedKeys = append(mergedKeys, key2)
				mergedData = append(mergedData, data2)
			} else {
				mergedKeys = append(mergedKeys, key1)
				mergedData = append(mergedData, data1)
			}
			toRead1 = true
			toRead2 = true
		} else if(key1 < key2){
			mergedKeys = append(mergedKeys, key1)
			mergedData = append(mergedData, data1)
			toRead1 = true
			toRead2 = false
		} else if(key2 < key1){
			mergedKeys = append(mergedKeys, key2)
			mergedData = append(mergedData, data2)
			toRead1 = false
			toRead2 = true
		}

	}
	file1.Close()
	file2.Close()
	return mergedKeys, mergedData
}

//Proveravamo da li smo prosli data zonu
func isEndOfData(file *os.File, dataEnd uint64) bool{
	currentOffset, err := file.Seek(0,1)
	if err != nil{
		log.Fatal(err)
	}
	if uint64(currentOffset) >= dataEnd {
		return true
	}
	return false
}

//Brise sstabelu
func deleteSSTable(directory string){
	err := os.RemoveAll("files/sstable/"+directory)
	if err != nil{
		log.Fatal(err)
	}
}

//Racuna velicinu sstabele za zadati nivo
func getSSTableSize(currentLevel uint32) uint32{
	config := GetConfig()
	//Racunamo velicinu naredne sstabele kao duplu od prethodne
	//jer ne znamo kolika ce tacno biti velicina,
	//mada ona nije ni toliko bitna jer je koristi samo bloomfilter za inicijalizaciju
	//On prima ocekivani broj elemenata tako da ovo nece biti greska
	return uint32(math.Pow(2, float64(currentLevel-1)) * float64(config.MemtableSize))
}

//Trazi kljuc unutar svih sstabela
func (lsm *Lsm) Find(key string) (bool, *Data){
	//iteriramo po nivoima
	for currentLevel:=uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++{
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		for i:=lsm.LevelSizes[currentLevel-1]; i > 0; i--{
			currentSSTable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, i))
			found, data := currentSSTable.Find(key)
			if found {
				return found, data
			}
		}
	}
	return false, nil

}

// ---------- SKENIRANJE VISE PODATAKA ----------

//iterira po svim sstabelama i prekida ako je napunio trazenu stranicu
func (lsm *Lsm) RangeScan(minKey string, maxKey string, scan *Scan) {
	//iteriramo po nivoima
	for currentLevel:=uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++{
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		for i:=lsm.LevelSizes[currentLevel-1]; i > 0; i--{
			currentSSTable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, i))
			currentSSTable.RangeScan(minKey,maxKey,scan)
			if scan.FoundResults >= scan.SelectedPageEnd{
				return
			}
		}
	}
}

//iterira po svim sstabelama i prekida ako je napunio trazenu stranicu
func (lsm *Lsm) ListScan(prefix string, scan *Scan) {
	//iteriramo po nivoima
	for currentLevel:=uint32(1); currentLevel <= lsm.MaxLevel; currentLevel++{
		size := getSSTableSize(currentLevel)
		//iteriramo po sstabelama kako su dodavane(od najveceg indeksa, noviji ce se prvi citati)
		for i:=lsm.LevelSizes[currentLevel-1]; i > 0; i--{
			currentSSTable := NewSSTable(size,lsm.GenerateSSTableName(currentLevel, i))
			currentSSTable.ListScan(prefix,scan)
			if scan.FoundResults >= scan.SelectedPageEnd{
				return
			}
		}
	}
}