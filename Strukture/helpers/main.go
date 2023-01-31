package main

import (
	"fmt"
	"project/gosaomi/b_tree"
	// . "project/gosaomi/dataType"
	. "project/gosaomi/lsm"
	// . "project/gosaomi/sstable"
)

func main() {

	bTree := b_tree.NewBTree(3)
	bTree.InsertElem("a", []byte("monke"), true)
	bTree.InsertElem("b", []byte("monke"))
	bTree.InsertElem("c", []byte("monke"))
	bTree.InsertElem("d", []byte("monke"))
	bTree.InsertElem("e", []byte("monke"))
	bTree.InsertElem("f", []byte("monke"))
	bTree.InsertElem("p", []byte("monke"))
	bTree.InsertElem("m", []byte("monke"))
	bTree.InsertElem("q", []byte("monk"))
	bTree.InsertElem("o", []byte("monke"))
	bTree.InsertElem("s", []byte("monke"))
	bTree.InsertElem("k", []byte("monke"))
	bTree.InsertElem("j", []byte("monk"))
	bTree.InsertElem("t", []byte("monke"))
	bTree.InsertElem("g", []byte("monk"))
	bTree.InsertElem("r", []byte("monke"))
	bTree.InsertElem("l", []byte("monke"))
	bTree.InsertElem("x", []byte("giraffe"))
	bTree.InsertElem("u", []byte("monk"))
	bTree.InsertElem("h", []byte("monke"))
	bTree.InsertElem("v", []byte("monke"))
	bTree.InsertElem("n", []byte("monke"))
	bTree.InsertElem("z", []byte("monke"))
	bTree.Remove("g")
	// bTree.PrintBTree()

	fmt.Println("====== DOBRODOSLI U KEY-VALUE ENGINE ======")
	for true {
		fmt.Println("1 - PUT")
		fmt.Println("2 - GET")
		fmt.Println("3 - DELETE")
		fmt.Println("4 - LIST")
		fmt.Println("5 - RANGE SCAN")
		fmt.Println("X - Izlaz iz programa")
		fmt.Println("=======================================")
		fmt.Print("Izaberite opciju: ")

		var input string
		n, err := fmt.Scanln(&input)

		if err != nil {
			fmt.Println("Greska prilikom unosa: ", err)
		} else if n == 0 {
			fmt.Println("Prazan unos.  Molimo vas probajte opet.")
			continue
		}

		switch input {
		case "1":
			fmt.Println("1")
		case "2":
			fmt.Println("2")
		case "3":
			fmt.Println("3")
		case "4":
			fmt.Println("4")
		case "5":
			fmt.Println("5")
		case "x":
			fmt.Println("Vidimo se sledeci put!")
			return
		case "X":
			fmt.Println("Vidimo se sledeci put!")
			return
		default:
			fmt.Println("Neispravan unos. Molimo vas probajte opet.")
		}
	}

	// keys := make([]string, 0)
	// values := make([]*Data, 0)
	// bTree.InorderTraverse(bTree.Root, &keys, &values)
	// for i := 0; i < len(keys); i++ {
	// 	fmt.Println("Key: ", keys[i], "Value: ", string(values[i].Value))
	// }

	//SSTABELA
	// sstable := NewSSTable(uint32(bTree.Size), "sstable-1")
	// sstable.Flush(keys, values)
	// sstable.ReadData()
	// sstable.ReadIndex()
	// sstable.ReadSummary()
	// sstable.ReadBloom()

	// found1, data1 := sstable.Find("b")
	// if !found1 {
	// 	fmt.Println("Nije pronadjen b")
	// } else {
	// 	fmt.Println(data1)
	// }

	// found2, data2 := sstable.Find("x")
	// if !found2 {
	// 	fmt.Println("Nije pronadjen x")
	// } else {
	// 	fmt.Println(data2)
	// }

	// found3, data3 := sstable.Find("aaaaaaa")
	// if !found3 {
	// 	fmt.Println("Nije pronadjen aaaaaaa")
	// } else {
	// 	fmt.Println(data3)
	// }

	InitializeLsm()
}
