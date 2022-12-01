package main

import "fmt"

func main() {
	a := make([]int, 5, 5)
	for i, val := range a {
		if val < i {
			fmt.Println(val)
			fmt.Println(i)
		}
	}
}
