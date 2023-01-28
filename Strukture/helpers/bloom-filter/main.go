package main

import (
	"fmt"
	"structures/bloom/bloom"
)

func main() {
	blm := bloom.NewBloomFilter(10, 2)
	blm.AddToBloom([]byte("MAJMUN"))
	blm.AddToBloom([]byte("filadendron"))
	fmt.Println(blm.IsInBloom([]byte("faradon")))
}
