package main

import (
	"fmt"
	"strings"
)

type BTreeNode struct {
	keys     []uint //Kljucevi
	values   map[uint][]byte
	children []*BTreeNode //Pokazivaci na decu
	parent   *BTreeNode
}

type BTree struct {
	root    *BTreeNode
	m       uint //Red stabla(maks broj dece)
	maxKeys uint //Maksimalan broj kljuceva
	size    uint //Broj elemenata u stablu
}

func newBTreeNode(parent *BTreeNode) *BTreeNode {
	bTreeNode := new(BTreeNode)
	bTreeNode.keys = make([]uint, 0)
	bTreeNode.values = make(map[uint][]byte)
	bTreeNode.children = make([]*BTreeNode, 0)
	bTreeNode.parent = parent
	return bTreeNode
}

// m = maksimalan broj dece
func newBTree(m uint) *BTree {
	bTree := new(BTree)
	bTree.root = nil
	bTree.m = m
	bTree.maxKeys = m - 1
	bTree.size = 0
	return bTree
}

// Nas bubblesort koji sortira uint
func BubbleSort(keys []uint) []uint {
	for i := 0; i < len(keys)-1; i++ {
		for j := 0; j < len(keys)-i-1; j++ {
			if keys[j] > keys[j+1] {
				keys[j], keys[j+1] = keys[j+1], keys[j]
			}
		}
	}
	return keys
}

// Brise element liste na datom indeksu
func RemoveIndex(s []uint, index int) []uint {
	return append(s[:index], s[index+1:]...)
}

// Trazi cvor sa kljucem
func (bTree *BTree) findNode(keyToFind uint) (bool, *BTreeNode) {
	//Da ne puca ako je prazan koren
	if bTree.root == nil {
		return false, nil
	}

	currentNode := bTree.root //Pocinjemo od korena
	for true {
		numberOfKeys := len(currentNode.keys) //Broj kljuceva za pretragu
		//Ukoliko nema kljuceva za pretragu
		// if numberOfKeys == 0 {
		// 	return false, currentNode
		// }
		//Iteriramo po kljucevima
		for index, key := range currentNode.keys {
			if key == keyToFind {
				return true, currentNode
			} else if keyToFind < key {
				if len(currentNode.children) == 0 {
					return false, currentNode //vracamo roditelja
				}
				currentNode = currentNode.children[index]
				break
			} else if keyToFind > key && index == numberOfKeys-1 {
				if len(currentNode.children) == 0 {
					return false, currentNode //vracamo roditelja
				}
				currentNode = currentNode.children[index+1]
				break
			} else if keyToFind > key && index != numberOfKeys-1 {
				continue
			}

		}
	}
	return false, currentNode
}

// Deli cvor rekurzivno do korena po potrebi
func (bTree *BTree) splitNode(node *BTreeNode) {
	parent := node.parent

	//Uslov za izlaz iz rekurzije, ako je dosao do korena
	if parent == nil {
		newRoot := newBTreeNode(nil)
		bTree.root = newRoot
		parent = newRoot
	}

	//Ukoliko pre nije imao nijedno dete
	if len(parent.children) == 0 {
		parent.children = append(parent.children, nil) //zauzimamo jedno mesto za desno dete
	}
	parent.children = append(parent.children, nil) //zauzimamo jedno mesto za levo dete

	//Delimo cvor na tri dela
	middleIndex := bTree.maxKeys / 2
	middleKey := node.keys[middleIndex]
	// middleVal := node.values[middleIndex]

	// ----- left -----
	leftNode := newBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[0:middleIndex] {
		leftNode.keys = append(leftNode.keys, key)
		leftNode.values[key] = node.values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[0 : middleIndex+1] {
			leftNode.children = append(leftNode.children, child)
		}
	}

	// ----- right -----
	rightNode := newBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[middleIndex+1:] {
		rightNode.keys = append(rightNode.keys, key)
		rightNode.values[key] = node.values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[middleIndex+1:] {
			rightNode.children = append(rightNode.children, child)
		}
	}

	// ----- Ubacujem srednji element u roditelja -----
	parent.keys = append(parent.keys, middleKey)
	parent.values[middleKey] = node.values[middleKey]
	parent.keys = BubbleSort(parent.keys)

	addedKeyIndex := 0
	for index, k := range parent.keys {
		if k == middleKey {
			addedKeyIndex = index
		}
	}
	//Pomeramo svu decu u desno
	// parent.children = append(parent.children, nil) //zauzimamo mesto
	for i := len(parent.children) - 2; i == addedKeyIndex; i++ {
		parent.children[i+1] = parent.children[i]
	}
	//Dodajemo podeljene node-ove kao decu
	parent.children[addedKeyIndex] = leftNode
	parent.children[addedKeyIndex+1] = rightNode

	//Proveravamo da li roditelj treba da se deli - REKURZIJA
	if len(parent.keys) > int(bTree.maxKeys) {
		bTree.splitNode(parent)
	}
}

// Treci parametar govori da li se rotira sa desnim rodjakom
// false znaci da se rotira sa levim
func (bTree *BTree) rotateNodes(node *BTreeNode, sibling *BTreeNode, isRight bool) {
	if isRight {
		//Najveceg iz roditelja spustamo
		keyFromParent := node.parent.keys[len(node.parent.keys)-1]
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.values[keyFromParent] = node.parent.values[keyFromParent]
		delete(node.parent.values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)

		//Najveceg iz naseg cvora dizemo
		keyFromNode := node.keys[len(node.keys)-1]
		node.parent.keys[len(node.parent.keys)-1] = keyFromNode //Prepisujemo preko starog
		node.parent.values[keyFromNode] = node.values[keyFromNode]
		delete(node.values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, len(node.keys)-1)
		node.parent.keys = BubbleSort(node.parent.keys)
	} else {
		//Najmanjeg iz roditelja spustamo
		keyFromParent := node.parent.keys[0]
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.values[keyFromParent] = node.parent.values[keyFromParent]
		delete(node.parent.values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)

		//Najmanjeg iz naseg cvora dizemo
		keyFromNode := node.keys[0]
		node.parent.keys[0] = keyFromNode //Prepisujemo preko starog
		node.parent.values[keyFromNode] = node.values[keyFromNode]
		delete(node.values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, len(node.keys)-1)
		node.parent.keys = BubbleSort(node.parent.keys)
	}
}

// Ubacuje kljuc
func (bTree *BTree) insertElem(key uint, val []byte) {

	//U slucaju da koren ne postoji
	if bTree.root == nil {
		bTree.root = newBTreeNode(nil) //Nema roditelja :(
		bTree.root.keys = append(bTree.root.keys, key)
		bTree.root.keys = BubbleSort(bTree.root.keys)
		bTree.root.values[key] = val
		bTree.size++
		return
	} else if len(bTree.root.keys) == int(bTree.maxKeys) {
		bTree.root.keys = append(bTree.root.keys, key)
		bTree.root.keys = BubbleSort(bTree.root.keys)
		bTree.root.values[key] = val
		bTree.size++
		bTree.splitNode(bTree.root)
		return
	}

	found, node := bTree.findNode(key)

	//Ukoliko vec postoji ne dodajemo
	if found {
		fmt.Printf("Kljuc '%d' vec postoji u stablu", key)
		return
	}

	//Dodamo element
	node.keys = append(node.keys, key)
	node.values[key] = val
	node.keys = BubbleSort(node.keys)
	bTree.size++

	//Ukoliko nema mesta u trenutnom cvoru
	if len(node.keys) > int(bTree.maxKeys) {
		siblings := node.parent.children
		for index, child := range siblings {
			//Trazim indeks trenutnog node-a
			if child == node {
				//Proveravam da li ima levog/desnog suseda
				if len(siblings) == 1 {
					break
				} else if index == 0 {
					rightSibling := siblings[index+1]
					if len(rightSibling.keys) == int(bTree.maxKeys) {
						break
					}
					//Rotacija...
					bTree.rotateNodes(node, rightSibling, true)
					return
				} else if index == len(siblings)-1 {
					leftSibling := siblings[index-1]
					if len(leftSibling.keys) == int(bTree.maxKeys) {
						break
					}
					//Rotacija...
					bTree.rotateNodes(node, leftSibling, false)
					return
				} else {
					leftSibling := siblings[index-1]
					if len(leftSibling.keys) < int(bTree.maxKeys) {
						//Rotacija
						bTree.rotateNodes(node, leftSibling, false)
						return
					}
					rightSibling := siblings[index+1]
					if len(rightSibling.keys) < int(bTree.maxKeys) {
						//Rotacija
						bTree.rotateNodes(node, rightSibling, true)
						return
					}
					break
				}
			}
		}
		//Ukoliko ne moze da rotira onda splituje
		bTree.splitNode(node)
	}
}

// DEVIOUS LICK
func (t *BTree) printBTree() {
	var queue []*BTreeNode
	queue = append(queue, t.root)
	level := 0

	for len(queue) > 0 {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[i]
			fmt.Print(strings.Repeat("  ", level))
			fmt.Print("Keys: ")
			for _, key := range current.keys {
				fmt.Print(key, " ")
			}
			fmt.Print(" | Children: ")
			for _, child := range current.children {
				queue = append(queue, child)
				fmt.Print(child, " ")
			}
			fmt.Println()
		}
		level++
		queue = queue[levelSize:]
	}
}
