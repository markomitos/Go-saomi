package main

import (
	"fmt"
	"math"
)

func noDigitis(num int) float64 {
	var i float64 = 0
	for num > 0 {
		num = num / 10
		i += 1
	}
	return i
}

func requiredSum(num int) float64 {
	var i float64 = noDigitis(num)
	var s float64 = 0
	var digit int

	for num > 0 {
		digit = num % 10
		num = num / 10
		s += math.Pow(float64(digit), i)
	}
	return s

}

func main() {
	var n int
	fmt.Println("Unesite broj:")
	fmt.Scan(&n)
	if n < 1 {
		fmt.Println("Unesite pozitivan broj!")
	} else {
		if int(requiredSum(n)) == n {
			fmt.Println("Jeste Armstrongov broj")
		} else {
			fmt.Println("Nije Armstrongov broj")
		}
	}
}
