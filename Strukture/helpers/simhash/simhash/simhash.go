package simhash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
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

func HashText(weightedMap map[string]int) []int {
	//Hash i konvertovanje u binarno
	hashedMap := make(map[string]string)
	for i, _ := range weightedMap {
		hashedMap[i] = ToBinary(GetMD5Hash(i))
	}

	//nule pretvaram u -1
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

	//Sabiram kolone pomnozene tezinom
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

// Ovo treba jos popraviti
func Compare(a []int, b []int) int {
	result := 0
	for i := 0; i < 256; i++ {
		if a[i] != b[i] {
			result++
		}
	}
	fmt.Println(result)
	return result
}
