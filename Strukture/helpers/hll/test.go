package hll

import (
	"fmt"
	"log"
)

func main() {
	hllA, err := NewHLL(10)
	if err != nil {
		log.Fatal(err)
	}

	// bs := make([]byte, 4)
	// binary.LittleEndian.PutUint32(bs, 1500)

	// for i := 0; i < 2000; i++ {
	// 	str := "masfasfg1245sgk" + "as" + strconv.Itoa(i)
	// 	hll.AddToHLL([]byte(str))
	// }

	// AddToHLL(hll, []byte(bs))
	hllA.AddToHLL("monke")
	// fmt.Println(hll.reg)
	hllA.AddToHLL("banan")

	fmt.Println(hllA.Estimate())

	bytes := HyperLogLogToBytes(hllA)
	hllB := BytesToHyperLogLog(bytes)
	fmt.Println(hllB.Estimate())

}
