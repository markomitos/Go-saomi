package main

import "fmt"

func main() {
	m := make(map[int]string)
	m[21000] = "Novi Sad"
	m[21101] = "Novi Sad"
	m[11000] = "Beograd"
	used := make([]string, 0, len(m))
	var isUsed bool = false
	for key, val := range m {
		isUsed = false
		for i := range used {
			if val == used[i] {
				isUsed = true
			}
		}
		if isUsed == false {
			used = append(used, val)
			fmt.Println(key, val)
		}
	}

	fmt.Println("")
}
