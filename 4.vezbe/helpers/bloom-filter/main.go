package main

import (
	"fmt"
)

func main() {
	// fns := CreateHashFunctions(5)

	// buf := &bytes.Buffer{}
	// encoder := gob.NewEncoder(buf)
	// decoder := gob.NewDecoder(buf)

	// for _, fn := range fns {
	// 	data := []byte("hello")
	// 	fmt.Println(fn.Hash(data))
	// 	err := encoder.Encode(fn)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	dfn := &HashWithSeed{}
	// 	err = decoder.Decode(dfn)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Println(dfn.Hash(data))
	// }
	b := newBloomFilter(10, 2)
	fmt.Println(isInBloom(b, []byte("MAJMUN")))
	addToBloom(b, []byte("MAJMUN"))
	addToBloom(b, []byte("filadendron"))
	fmt.Println(isInBloom(b, []byte("MAJMUN")))
	fmt.Println(isInBloom(b, []byte("MAJMUNE")))
	fmt.Println(isInBloom(b, []byte("filadendron")))
	fmt.Println(isInBloom(b, []byte("ker")))

}
