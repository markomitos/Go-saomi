package main

import (
	"fmt"
	"log"
)

func main() {
	hll, e := newHLL(4)
	if e != nil {
		log.Fatal(e)
	}
	hll.addToHLL("majmune")
	hll.addToHLL("meme")
	hll.addToHLL("bruh")
	hll.addToHLL("ajme")
	hll.addToHLL("jaje")
	fmt.Println(hll.reg)
	fmt.Println(hll.Estimate())
}
