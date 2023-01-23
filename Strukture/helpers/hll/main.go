package main

import (
	"fmt"
	"log"
)

func main() {
	hll, err := newHLL(10)
	if err != nil {
		log.Fatal(err)
	}

	// bs := make([]byte, 4)
	// binary.LittleEndian.PutUint32(bs, 1500)

	// for i := 0; i < 2000; i++ {
	// 	str := "masfasfg1245sgk" + "as" + strconv.Itoa(i)
	// 	hll.addToHLL([]byte(str))
	// }

	// addToHLL(hll, []byte(bs))
	hll.addToHLL("monke")
	// fmt.Println(hll.reg)
	hll.addToHLL("banan")
	// fmt.Println(hll.reg)
	hll.addToHLL("ap45ple")
	// fmt.Println(hll.reg)
	hll.addToHLL("ora78nge")
	// fmt.Println(hll.reg)
	hll.addToHLL("cucum1245ber")
	// fmt.Println(hll.reg)
	hll.addToHLL("2778")
	hll.addToHLL("48049894811984")
	hll.addToHLL("7/91*/")
	hll.addToHLL("71967678/")
	fmt.Println(hll.reg)

	fmt.Println(hll.Estimate())

}
