package main

import "fmt"

func main() {
	cms := newCountMinSketch(0.1, 0.9)
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("banana"))
	addToCms(cms, []byte("drvo"))
	addToCms(cms, []byte("kokos"))
	addToCms(cms, []byte("kokos"))
	fmt.Println(cms.valueTable)

	fmt.Println(checkFrequencyInCms(cms, []byte("majmun")))
	fmt.Println(checkFrequencyInCms(cms, []byte("drvo")))

}
