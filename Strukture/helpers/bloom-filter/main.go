package main

import (
	"example/bloom/bloomfilter"
	"fmt"
)

func main() {
	blm := bloomfilter.NewBloomFilter(10, 2)
	bloomfilter.AddToBloom(blm, []byte("MAJMUN"))
	bloomfilter.AddToBloom(blm, []byte("filadendron"))
	fmt.Println(bloomfilter.IsInBloom(blm, []byte("faradon")))
}
