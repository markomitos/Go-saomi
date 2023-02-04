package merkle

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type Node struct {
	Data  []byte
	hash  [20]byte
	left  *Node
	right *Node
}

type MerkleRoot struct {
	Root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.Root.String()
}

func ReadFile(fileName string) []*Node {
	// Ucitavanje ulaznih podataka u listu nodova
	nodes := make([]*Node, 0)
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		string_data := scanner.Text()

		node := Node{Data: []byte(string_data)}
		nodes = append(nodes, &node)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
	return nodes
}
func WriteFile(file *os.File, rootNode *Node) {

	// Create a new bufio.Writer
	writer := bufio.NewWriter(file)

	// Write a string to the file
	_, err := writer.WriteString(rootNode.String())
	if err != nil {
		fmt.Println(err)
		return
	}

	// Flush the buffer to the file
	err = writer.Flush()
	if err != nil {
		log.Fatal(err)
	}
}

func MakeMerkel(nodes []*Node) *MerkleRoot {

	// Prolazi kroz nodove koristi pomocnu listu
	// smanjuje je i kada dostigne velicinu 1 vraca Root node
	for len(nodes) > 1 {
		newNodes := make([]*Node, 0)
		for i := 0; i < len(nodes); i += 2 {
			n := new(Node)
			n.left = nodes[i]
			if i+1 > len(nodes)-1 {
				n.right = &Node{hash: Hash([]byte{})}
			} else {
				n.right = nodes[i+1]
			}
			Data := make([]byte, 0)
			Data = append(Data, n.left.Data...)
			Data = append(Data, n.right.Data...)
			n.hash = Hash(Data)
			newNodes = append(newNodes, n)
		}
		nodes = newNodes
	}
	Root := &MerkleRoot{Root: nodes[0]}
	return Root
}

func (n *Node) String() string {
	return hex.EncodeToString(n.hash[:])
}

func (n *Node) Data_String() string {
	return string(n.Data[:])
}

func Hash(Data []byte) [20]byte {
	return sha1.Sum(Data)
}

// func main() {

// 	mr := &MerkleRoot{}

// 	// Ovo odkomentarisemo ako zelimo da koristimo podatke iz ulaznog txt
// 	// Koristio sam ovako u tri primera dole jer mi je bilo lakse da testiram
// 	//nodes := readFile("Data.txt")

// 	// Testni podaci
// 	leaf1 := &Node{Data: []byte("data1")}
// 	leaf2 := &Node{Data: []byte("data2")}
// 	leaf3 := &Node{Data: []byte("data3")}

// 	nodes_pom := []Node{*leaf1, *leaf2, *leaf3}

// 	//Ovo takodje odkomentarisemo kako bismo radili sa ulaznim podacima
// 	//root_node := makeMerkel(nodes)
// 	root_node := MakeMerkel(nodes_pom)

// 	WriteFile("metadata.txt", *root_node.Root)

// 	fmt.Println("Ovo je test funkcije : ", root_node.String())

// 	// Ovde proveravamo jer ovi ispod izrazi daju tacno resenje

// 	leaf1.hash = Hash(leaf1.Data)
// 	leaf2.hash = Hash(leaf2.Data)
// 	leaf3.hash = Hash(leaf3.Data)

// 	parent1 := &Node{left: leaf1, right: leaf2, hash: Hash(append(leaf1.Data[:], leaf2.Data[:]...))}
// 	parent2 := &Node{left: leaf3, hash: Hash(leaf3.Data[:])}

// 	mr.Root = &Node{left: parent1, right: parent2, hash: Hash(append(parent1.Data[:], parent2.Data[:]...))}

// 	fmt.Println("\nVrednost leaf1 node-a: ", leaf1.Data_String())
// 	fmt.Println("\nHash Root node-a: ", mr.String())

// }
