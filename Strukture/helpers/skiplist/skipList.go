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
	key       string
	value     []byte
	timestamp []byte
	tombstone bool
	next      []*SkipListNode
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

func NewSkipList(maxh int) *SkipList {
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

func (s *SkipList) Put(key string, value []byte, timestamp []byte) {
	node, found := s.find(key)
	//update ako ga je nasao
	if found {
		node.value = value
	} else {
		//Pravimo nov node
		level := s.roll()
		newNode := &SkipListNode{
			key:       key,
			value:     value,
			timestamp: timestamp,
			tombstone: false,
			next:      make([]*SkipListNode, level),
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

func (s *SkipList) Remove(key string) {
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

// uzima sve podatke u sortiranom redosledu
func (s *SkipList) GetAllNodes() map[string][]byte {
	var nodeList = make(map[string][]byte)
	currentNode := s.head
	for currentNode.next[0] != nil {
		next := currentNode.next[0]
		nodeList[next.key] = next.value
		currentNode = next
	}

	return nodeList
}

func (s *SkipList) Print() {
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
	s := NewSkipList(10)
	s.Put("i", []byte("majmun"), []byte("vreme"))
	s.Put("c", []byte("majmun"), []byte("vreme"))
	s.Put("e", []byte("majmun"), []byte("vreme"))
	s.Put("d", []byte("majmun"), []byte("vreme"))
	s.Put("f", []byte("majmun"), []byte("vreme"))
	s.Put("g", []byte("majmun"), []byte("vreme"))
	s.Put("s", []byte("majmun"), []byte("vreme"))
	s.Put("q", []byte("majmun"), []byte("vreme"))
	s.Put("r", []byte("majmun"), []byte("vreme"))
	s.Put("t", []byte("majmun"), []byte("vreme"))
	s.Put("j", []byte("majmun"), []byte("vreme"))
	s.Put("l", []byte("majmun"), []byte("vreme"))
	s.Put("p", []byte("majmun"), []byte("vreme"))
	s.Put("o", []byte("majmun"), []byte("vreme"))
	s.Print()
	// s.remove("b")
	// s.print()
	// s.remove("g")
	// s.print()
	// s.put("a", []byte("tigar"))
	// s.put("nm", []byte("tigar"))
	// fmt.Println(s.height)
	// s.print()

	// fmt.Println(s.find("22"))
}
