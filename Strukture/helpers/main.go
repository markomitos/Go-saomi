package main

import (
	"fmt"
	"project/gosaomi/b_tree"
	. "project/gosaomi/dataType"
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
	bTree.InsertElem("x", []byte("monke"))
	bTree.InsertElem("u", []byte("monk"))
	bTree.InsertElem("h", []byte("monke"))
	bTree.InsertElem("v", []byte("monke"))
	bTree.InsertElem("n", []byte("monke"))
	bTree.InsertElem("z", []byte("monke"))
	bTree.Remove("g")
	bTree.PrintBTree()

	keys := make([]string, 0)
	values := make([]*Data, 0)
	bTree.InorderTraverse(bTree.Root, &keys, &values)
	for i := 0; i < len(keys); i++ {
		fmt.Println("Key: ", keys[i], "Value: ", string(values[i].Value))
	}
}
