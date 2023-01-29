package simhash

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// "D:\\GO\\Strukture\\helpers\\simhash\\tekst1.txt"

func parseFileToWeightedMap(file string) map[string]int {
	mapa := make(map[string]int)
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	//Citamo rec po rec koristeci scanner
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		tmp := strings.ToUpper(scanner.Text())
		tmp = strings.Trim(tmp, ",")
		tmp = strings.Trim(tmp, ".")
		tmp = strings.Trim(tmp, "!")
		tmp = strings.Trim(tmp, "?")

		//Provera kljuca
		i := mapa[tmp]
		if i == 0 {
			mapa[tmp] = 1
		} else {
			mapa[tmp]++
		}
	}
	f.Close()
	return mapa
}

func main() {
	txt1, err1 := filepath.Abs("tekst1.txt")
	if err1 != nil {
		log.Fatal()
	}
	txt2, err2 := filepath.Abs("tekst2.txt")
	if err2 != nil {
		log.Fatal()
	}
	txt3, err3 := filepath.Abs("tekst3.txt")
	if err3 != nil {
		log.Fatal()
	}

	fmt.Println(txt1)
	mapa1 := parseFileToWeightedMap(txt1)
	mapa2 := parseFileToWeightedMap(txt2)
	mapa3 := parseFileToWeightedMap(txt3)
	fmt.Println(mapa1)

	mapa3["ABOUT"]++

	Compare(HashText(mapa1), HashText(mapa2))
	Compare(HashText(mapa1), HashText(mapa3))

}
