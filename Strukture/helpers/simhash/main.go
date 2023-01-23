package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
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
	mapa1 := parseFileToWeightedMap("D:\\GO\\Strukture\\helpers\\simhash\\tekst1.txt")
	mapa2 := parseFileToWeightedMap("D:\\GO\\Strukture\\helpers\\simhash\\tekst2.txt")
	mapa3 := parseFileToWeightedMap("D:\\GO\\Strukture\\helpers\\simhash\\tekst1.txt")
	fmt.Println(mapa1)

	mapa3["ABOUT"]++

	compare(hashText(mapa1), hashText(mapa2))
	compare(hashText(mapa1), hashText(mapa3))

}
