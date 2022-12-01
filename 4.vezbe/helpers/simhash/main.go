package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// "D:\\GO\\Strukture\\helpers\\simhash\\tekst1.txt"

func parseFileToWightedMap(file string) map[string]int {
	mapa := make(map[string]int)
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// Citamo rec po rec koristeci scanner
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		tmp := strings.ToUpper(scanner.Text())
		tmp = strings.Trim(tmp, ",")
		tmp = strings.Trim(tmp, ".")
		tmp = strings.Trim(tmp, "!")
		tmp = strings.Trim(tmp, "?")

		// Provera kljuca
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
	mapa1 := parseFileToWightedMap("C:\\Users\\marko\\Desktop\\fakultet\\3. semestar\\NASP\\4.vezbe\\helpers\\simhash\\tekst1.txt")
	mapa2 := parseFileToWightedMap("C:\\Users\\marko\\Desktop\\fakultet\\3. semestar\\NASP\\4.vezbe\\helpers\\simhash\\tekst2.txt")
	fmt.Println(mapa1)

	hashText(mapa1)
	hashText(mapa2)
	compare(hashText(mapa1), hashText(mapa2))
	// fmt.Println(mapa1)
	// fmt.Println(mapa2)
	// fmt.Println(GetMD5Hash("hello"))
	// fmt.Println(ToBinary(GetMD5Hash("hello")))
}
