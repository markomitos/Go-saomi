package main

import "fmt"

func main() {
	blm := newBloomFilter(10, 2)
	addToBloom(blm, []byte("MAJMUN"))
	addToBloom(blm, []byte("filadendron"))
	fmt.Println(isInBloom(blm, []byte("faradon")))
	fmt.Println(blm.bitset)
}
