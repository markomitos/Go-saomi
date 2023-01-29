package b_tree

import (
	. "dataType"
	"fmt"
	"strings"
	"time"
)

type BTreeNode struct {
	keys     []string //Kljucevi
	values   map[string]*Data
	children []*BTreeNode //Pokazivaci na decu
	parent   *BTreeNode
}

type BTree struct {
	root    *BTreeNode
	m       uint //Red stabla(maks broj dece)
	maxKeys uint //Maksimalan broj kljuceva
	size    uint //Broj elemenata u stablu
}

func NewBTreeNode(parent *BTreeNode) *BTreeNode {
	bTreeNode := new(BTreeNode)
	bTreeNode.keys = make([]string, 0)
	bTreeNode.values = make(map[string]*Data)
	bTreeNode.children = make([]*BTreeNode, 0)
	bTreeNode.parent = parent
	return bTreeNode
}

// m = maksimalan broj dece
func NewBTree(m uint) *BTree {
	bTree := new(BTree)
	bTree.root = nil
	bTree.m = m
	bTree.maxKeys = m - 1
	bTree.size = 0
	return bTree
}

// Nas bubblesort koji sortira uint
func BubbleSort(keys []string) []string {
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
func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

// Trazi cvor sa kljucem
func (bTree *BTree) FindNode(keyToFind string) (bool, *BTreeNode) {
	//Da ne puca ako je prazan koren
	if bTree.root == nil {
		return false, nil
	}

	currentNode := bTree.root //Pocinjemo od korena
	for true {
		numberOfKeys := len(currentNode.keys) //Broj kljuceva za pretragu

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
		newRoot := NewBTreeNode(nil)
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
	leftNode := NewBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[0:middleIndex] {
		leftNode.keys = append(leftNode.keys, key)
		leftNode.values[key] = node.values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[0 : middleIndex+1] {
			leftNode.children = append(leftNode.children, child)
			child.parent = leftNode
		}
	}

	// ----- right -----
	rightNode := NewBTreeNode(parent)
	//dodajem kljuceve i vrednosti
	for _, key := range node.keys[middleIndex+1:] {
		rightNode.keys = append(rightNode.keys, key)
		rightNode.values[key] = node.values[key]
	}
	//dodajem decu, ukoliko je ima
	if len(node.children) != 0 {
		for _, child := range node.children[middleIndex+1:] {
			rightNode.children = append(rightNode.children, child)
			child.parent = rightNode
		}
	}

	// ----- Ubacujem srednji element u roditelja -----
	parent.keys = append(parent.keys, middleKey)
	parent.values[middleKey] = node.values[middleKey]
	parent.keys = BubbleSort(parent.keys)

	// //Brisem stari cvor
	// delete(node.values, middleKey) //Brisemo value iz naseg cvora
	// node.keys = RemoveIndex(node.keys, int(middleIndex))
	// node.keys = BubbleSort(node.keys)

	addedKeyIndex := 0
	for index, k := range parent.keys {
		if k == middleKey {
			addedKeyIndex = index
			break
		}
	}
	//Pomeramo svu decu u desno
	// parent.children = append(parent.children, nil) //zauzimamo mesto

	for i := len(parent.children) - 2; i > addedKeyIndex; i-- {
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
		//Najveceg iz naseg cvora dizemo
		keyFromNode := node.keys[len(node.keys)-1]

		keyFromParent := ""
		indexFromParent := 0
		//Trazim kojeg iz roditeljskog spustam dole(prvog veceg)
		for index, key := range node.parent.keys {
			if key > keyFromNode {
				keyFromParent = key
				indexFromParent = int(index)
				break
			}
		}

		node.parent.keys[indexFromParent] = keyFromNode //Prepisujemo preko starog
		node.parent.values[keyFromNode] = node.values[keyFromNode]
		delete(node.values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, len(node.keys)-1)
		node.parent.keys = BubbleSort(node.parent.keys)

		//Prvog najveceg iz roditelja spustamo
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.values[keyFromParent] = node.parent.values[keyFromParent]
		delete(node.parent.values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)

	} else {
		//Najmanjeg iz naseg cvora dizemo
		keyFromNode := node.keys[0]

		keyFromParent := ""
		indexFromParent := 0
		//Trazim kojeg iz roditeljskog spustam dole(prvog manjeg)
		for index := len(node.parent.keys) - 1; index >= 0; index-- {
			if node.parent.keys[index] < keyFromNode {
				keyFromParent = node.parent.keys[index]
				indexFromParent = int(index)
				break
			}
		}

		node.parent.keys[indexFromParent] = keyFromNode //Prepisujemo preko starog
		node.parent.values[keyFromNode] = node.values[keyFromNode]
		delete(node.values, keyFromNode) //Brisemo value iz naseg cvora
		node.keys = RemoveIndex(node.keys, 0)
		node.parent.keys = BubbleSort(node.parent.keys)

		//Prvog najmanjeg iz roditelja spustamo
		sibling.keys = append(sibling.keys, keyFromParent)
		sibling.values[keyFromParent] = node.parent.values[keyFromParent]
		delete(node.parent.values, keyFromParent) //Brisemo value iz roditelja
		sibling.keys = BubbleSort(sibling.keys)
	}
}

// Ubacuje kljuc
func (bTree *BTree) InsertElem(key string, val []byte, tombstone ...bool) {
	data := new(Data)
	data.Value = val
	data.Tombstone = false
	data.Timestamp = uint64(time.Now().Unix())
	if len(tombstone) > 0 {
		data.Tombstone = tombstone[0]
	}

	//U slucaju da koren ne postoji
	if bTree.root == nil {
		bTree.root = NewBTreeNode(nil) //Nema roditelja :(
		bTree.root.keys = append(bTree.root.keys, key)
		bTree.root.keys = BubbleSort(bTree.root.keys)
		bTree.root.values[key] = data
		bTree.size++
		return
	}

	found, node := bTree.FindNode(key)

	//Ukoliko vec postoji ne dodajemo
	if found {
		fmt.Printf("Kljuc '%s' vec postoji u stablu\n", key)
		return
	}

	//Dodamo element
	node.keys = append(node.keys, key)
	node.values[key] = data
	node.keys = BubbleSort(node.keys)
	bTree.size++

	//Ukoliko nema mesta u trenutnom cvoru
	if len(node.keys) > int(bTree.maxKeys) {
		if node.parent != nil {
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
		}

		//Ukoliko ne moze da rotira onda splituje
		bTree.splitNode(node)
	}
}

// Logicko brisanje - postavlja tombstone na true
func (bTree *BTree) Remove(key string) {
	found, node := bTree.FindNode(key)
	if !found {
		return
	}
	node.values[key].Tombstone = true
}

// INORDER obilazak
func (t *BTree) InOrder() []map[string]*Data {
	result := make([]map[string]*Data, 0)
	inOrderTraversal(t.root, &result)
	return result
}

func inOrderTraversal(node *BTreeNode, result *[]map[string]*Data) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.children); i++ {
		inOrderTraversal(node.children[i], result)
	}

	for _, key := range node.keys {
		*result = append(*result, map[string]*Data{key: node.values[key]})
	}

	for i := len(node.children) - 1; i >= 0; i-- {
		inOrderTraversal(node.children[i], result)
	}
}

// DEVIOUS LICK
func (t *BTree) PrintBTree() {
	var queue []*BTreeNode
	queue = append(queue, t.root)
	level := 0

	for len(queue) > 0 {
		fmt.Println("----------------------------------------")
		fmt.Println("Level: ", level)
		fmt.Println("----------------------------------------")

		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[i]
			fmt.Print(strings.Repeat("  ", level))
			fmt.Print("Keys: ")
			for _, key := range current.keys {
				if current.values[key].Tombstone {
					fmt.Print("(", key, ")", " ")
				} else {
					fmt.Print(key, " ")
				}
			}
			fmt.Print(" | Children: ")
			for _, child := range current.children {
				queue = append(queue, child)
				fmt.Print(child.keys, " ")
			}
			if current.parent != nil {
				fmt.Print(" | Parent: ", current.parent.keys)
			}
			fmt.Println()
		}
		level++
		queue = queue[levelSize:]
	}
}
