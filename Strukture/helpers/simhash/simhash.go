package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
)

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}
//pretvara niz bajtova u mapu
//kljuc - rec
//vrednost - broj ponavljanja te reci
func GenerateWeightedMap(bytes []byte) map[string]int {
	mapa := make(map[string]int)

	text := string(bytes)

	//string pretvorimo u niz reci
	words := strings.Split(text, " ")

	for _, word := range words {
		word := strings.ToUpper(word)
		word = strings.Trim(word, ",")
		word = strings.Trim(word, ".")
		word = strings.Trim(word, "!")
		word = strings.Trim(word, "?")

		//Provera kljuca
		i := mapa[word]
		if i == 0 {
			mapa[word] = 1
		} else {
			mapa[word]++
		}
	}
	return mapa
}

func HashText(weightedMap map[string]int) []int {
	//Hash i konvertovanje u binarno
	hashedMap := make(map[string]string)
	for i, _ := range weightedMap {
		hashedMap[i] = ToBinary(GetMD5Hash(i))
	}

	//nule pretvara u -1
	valueMap := make(map[string][]int)
	for word, bitset := range hashedMap {
		valueMap[word] = make([]int, 256)
		for index, bit := range bitset {
			// fmt.PrintLn(valueMap[i])
			if bit == '0' {
				valueMap[word][index] = -1
			} else {
				valueMap[word][index] = 1
			}
		}
	}

	//Sabira kolone pomnozene tezinom
	sumArray := make([]int, 256)
	for i := 0; i < 256; i++ {
		for word, _ := range valueMap {
			sumArray[i] += (valueMap[word][i] * weightedMap[word])
		}
	}

	//Pozitivne vrednosti --> 1
	//Negativne vrednosti --> 0
	for i, num := range sumArray {
		if num > 0 {
			sumArray[i] = 1
		} else {
			sumArray[i] = 0
		}
	}
	return sumArray
}

//poredi dva niza bajtova i vraca hemingovo rastojanje
func Compare(val1 []byte, val2 []byte) int {
	
	a := HashText(GenerateWeightedMap(val1))
	b := HashText(GenerateWeightedMap(val2))

	result := 0
	for i := 0; i < 256; i++ {
		if a[i] != b[i] {
			result++
		}
	}
	fmt.Println(result)
	return result
}
