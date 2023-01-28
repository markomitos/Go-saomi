package main

import (
	"fmt"
	"structures/bloom/bloomfilter"
)

func main() {
	blm := bloomfilter.NewBloomFilter(10, 2)
	blm.AddToBloom([]byte("MAJMUN"))
	blm.AddToBloom([]byte("filadendron"))
	fmt.Println(blm.IsInBloom([]byte("faradon")))
}
