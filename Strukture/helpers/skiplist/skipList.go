package skiplist

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	//key uint da bi bilo isto kao kod B_tree
	key   string
	value []byte
	next  []*SkipListNode
}

func (s *SkipList) roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

func newSkipList(maxh int) *SkipList {
	skipList := new(SkipList)
	skipList.maxHeight = maxh
	skipList.height = 0
	skipList.size = 0
	skipList.head = &SkipListNode{
		key:   "",
		value: []byte(nil),
		next:  make([]*SkipListNode, maxh),
	}
	return skipList
}

func (s *SkipList) find(key string) (*SkipListNode, bool) {
	currentNode := s.head
	for currentLevel := s.height - 1; currentLevel >= 0; currentLevel-- {
		if currentNode.key == key {
			return currentNode, true
		} else if currentNode.key < key {
			for currentNode.key <= key {
				if currentNode.key == key {
					return currentNode, true
				}
				next := currentNode.next[currentLevel]
				if next == nil || next.key > key {
					break
				}
				currentNode = next
			}
		}
	}
	return currentNode, false
}

// Prevezuje sve pokazivace nakon ubacivanja
func (s *SkipList) updateNodePointers(node *SkipListNode, minHeight int) {
	currentNode := s.head
	nodeHeight := len(node.next)
	key := node.key
	for currentLevel := nodeHeight - 1; currentLevel > minHeight; currentLevel-- {
		if currentNode.key < key {
			for currentNode.key < key {
				next := currentNode.next[currentLevel]
				//Pre nego sto se spustamo nivo dole prevezemo pokazivace
				if next == nil || next.key > key {
					tempNextNode := next
					currentNode.next[currentLevel] = node
					node.next[currentLevel] = tempNextNode
					break
				}
				currentNode = next
			}
		}
	}
}

func (s *SkipList) put(key string, value []byte) {
	node, found := s.find(key)
	//update ako ga je nasao
	if found {
		node.value = value
	} else {
		//Pravimo nov node
		level := s.roll()
		newNode := &SkipListNode{
			key:   key,
			value: value,
			next:  make([]*SkipListNode, level),
		}

		//Prevezujemo pokazivace do visine pronadjenog node-a
		for currentLevel := int(math.Min(float64(len(node.next)), float64(level))) - 1; currentLevel >= 0; currentLevel-- {
			tempNextNode := node.next[currentLevel]
			node.next[currentLevel] = newNode
			newNode.next[currentLevel] = tempNextNode
		}
		//Prevezujemo preostale pokazivace
		//u slucaju da je visina naseg node-a veca od pronadjenog
		if len(node.next) < level {
			s.updateNodePointers(newNode, len(node.next)-1)
		}
	}
}

func (s *SkipList) updateHeight() {
	for currentLevel := s.height - 1; currentLevel >= 0; currentLevel-- {
		if s.head.next[currentLevel] == nil {
			s.height--
		} else {
			break
		}
	}
}

func (s *SkipList) remove(key string) {
	node, found := s.find(key)
	currentNode := s.head
	if found {
		//Prevezujemo pokazivace do visine pronadjenog node-a
		for currentLevel := len(node.next) - 1; currentLevel >= 0; currentLevel-- {
			for currentNode != nil {
				if currentNode.next[currentLevel].key == key {
					//Prevezi
					currentNode.next[currentLevel] = currentNode.next[currentLevel].next[currentLevel]
					break
				}
				currentNode = currentNode.next[currentLevel]
			}
		}
	}
	s.updateHeight()
}

// func (s *SkipList) oldPrint() {
// 	fmt.Println("-------------------------------------------------------------")
// 	for currentLevel := s.height - 1; currentLevel >= 0; currentLevel-- {
// 		currentNode := s.head
// 		fmt.Print("head")
// 		for currentNode != nil {
// 			fmt.Print(currentNode.key)
// 			fmt.Print(" -> ")
// 			currentNode = currentNode.next[currentLevel]
// 		}
// 		fmt.Print("nil")
// 		fmt.Println()
// 	}
// 	fmt.Println("-------------------------------------------------------------")
// }

func (s *SkipList) print() {
	fmt.Println(strings.Repeat("_", 100))
	fmt.Println()
	currentNode := s.head.next[0]
	//level zero nodes
	nodeSlice := make([]*SkipListNode, 0)
	for currentNode != nil {
		nodeSlice = append(nodeSlice, currentNode)
		currentNode = currentNode.next[0]
	}

	for currentLevel := s.height - 1; currentLevel >= 0; currentLevel-- {
		fmt.Print("head -")
		for i := 0; i < len(nodeSlice); i++ {
			if len(nodeSlice[i].next) > currentLevel {
				fmt.Print("> " + nodeSlice[i].key)
				fmt.Print(" -")
			} else {
				keyLen := len(nodeSlice[i].key)
				fmt.Print(strings.Repeat("-", keyLen+4))
			}
		}
		fmt.Print("> nil")
		fmt.Println()
	}
	fmt.Println(strings.Repeat("_", 100))
}

func main() {
	s := newSkipList(10)
	s.put("i", []byte("majmun"))
	s.put("c", []byte("majmun"))
	s.put("e", []byte("majmun"))
	s.put("d", []byte("majmun"))
	s.put("f", []byte("majmun"))
	s.put("g", []byte("majmun"))
	s.put("s", []byte("majmun"))
	s.put("q", []byte("majmun"))
	s.put("r", []byte("majmun"))
	s.put("t", []byte("majmun"))
	s.put("j", []byte("majmun"))
	s.put("l", []byte("majmun"))
	s.put("p", []byte("majmun"))
	s.put("o", []byte("majmun"))
	s.print()
	// s.remove("b")
	// s.print()
	// s.remove("g")
	// s.print()
	// s.put("a", []byte("tigar"))
	// s.put("nm", []byte("tigar"))
	// s.put("daf", []byte("tigar"))
	// fmt.Println(s.height)
	// s.print()

	// fmt.Println(s.find("22"))
}
