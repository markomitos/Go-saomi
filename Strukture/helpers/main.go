package main

import (
	"fmt"
	"project/gosaomi/b_tree"
	. "project/gosaomi/dataType"
	. "project/gosaomi/sstable"
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

	keys := make([]string, 0)
	values := make([]*Data, 0)
	bTree.InorderTraverse(bTree.Root, &keys, &values)
	for i := 0; i < len(keys); i++ {
		fmt.Println("Key: ", keys[i], "Value: ", string(values[i].Value))
	}

	//SSTABELA
	sstable := NewSSTable(uint32(bTree.Size), "sstable-1")
	sstable.Flush(keys, values)
	// sstable.ReadData()
	// sstable.ReadIndex()
	// sstable.ReadSummary()
	// sstable.ReadBloom()

	found1, data1 := sstable.Find("b")
	if !found1 {
		fmt.Println("Nije pronadjen b")
	} else {
		fmt.Println(data1)
	}

	found2, data2 := sstable.Find("x")
	if !found2 {
		fmt.Println("Nije pronadjen x")
	} else {
		fmt.Println(data2)
	}

	found3, data3 := sstable.Find("aaaaaaa")
	if !found3 {
		fmt.Println("Nije pronadjen aaaaaaa")
	} else {
		fmt.Println(data3)
	}
}