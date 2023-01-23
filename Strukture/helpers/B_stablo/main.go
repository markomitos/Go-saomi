package main

func main() {
	bTree := newBTree(3)
	bTree.insertElem(3, []byte("monke"))
	bTree.insertElem(20, []byte("monke"))
	bTree.insertElem(15, []byte("monke"))
	bTree.insertElem(7, []byte("monke"))
	bTree.insertElem(8, []byte("monke"))
	bTree.printBTree()
}
