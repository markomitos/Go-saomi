package main

import "fmt"

func main() {
	cms := newCountMinSketch(0.1, 0.9)
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("majmun"))
	addToCms(cms, []byte("banana"))
	addToCms(cms, []byte("banana"))
	addToCms(cms, []byte("drvo"))

	fmt.Println(checkFrequencyInCms(cms, []byte("majmun")))
}
