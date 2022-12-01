package main

import "fmt"

func main() {
	var n int
	fmt.Println("Unesite broj: ")
	fmt.Scan(&n)
	if n < 1 {
		fmt.Println("Unesite pozitivan broj!")
	} else {
		fmt.Println(prime(n))
	}
}

func prime(n int) int {
	var prim int = -1
	var num int = 2
	for n > 0 {
		var isPrime bool = true
		for i := 2; i < num; i++ {
			if num%i == 0 {
				isPrime = false
			}
		}
		if isPrime == true {
			prim = num
			n -= 1
		}
		num++
	}
	return prim
}
