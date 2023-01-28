package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

func ReadFile(fileName string) []Node {
	// Ucitavanje ulaznih podataka u listu nodova
	nodes := []Node{}
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		string_data := scanner.Text()

		node := Node{data: []byte(string_data)}
		nodes = append(nodes, node)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	return nodes
}
func WriteFile(fileName string, rootNode Node) {

	// Verovatno beskorisno ali za relativni path
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		return
	}
	filePath := filepath.Join(dir, fileName)

	// Open a file for writing
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Create a new bufio.Writer
	writer := bufio.NewWriter(file)

	// Write a string to the file
	_, err = writer.WriteString(rootNode.String())
	if err != nil {
		fmt.Println(err)
		return
	}

	// Flush the buffer to the file
	writer.Flush()
}

func MakeMerkel(nodes []Node) MerkleRoot {

	// Prolazi kroz nodove koristi pomocnu listu
	// smanjuje je i kada dostigne velicinu 1 vraca root node
	for len(nodes) > 1 {
		var newNodes []Node
		for i := 0; i < len(nodes); i += 2 {
			n := &Node{}
			n.left = &nodes[i]
			if i+1 > len(nodes)-1 {
				n.right = &Node{hash: Hash([]byte{})}
			} else {
				n.right = &nodes[i+1]
			}
			data := make([]byte, 0)
			data = append(data, n.left.data...)
			data = append(data, n.right.data...)
			n.hash = Hash(data)
			newNodes = append(newNodes, *n)
		}
		nodes = newNodes
	}
	root := &MerkleRoot{root: &nodes[0]}
	return *root
}

type Node struct {
	data  []byte
	hash  [20]byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.hash[:])
}

func (n *Node) Data_String() string {
	return string(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func main() {

	mr := &MerkleRoot{}

	// Ovo odkomentarisemo ako zelimo da koristimo podatke iz ulaznog txt
	// Koristio sam ovako u tri primera dole jer mi je bilo lakse da testiram
	//nodes := readFile("data.txt")

	// Testni podaci
	leaf1 := &Node{data: []byte("data1")}
	leaf2 := &Node{data: []byte("data2")}
	leaf3 := &Node{data: []byte("data3")}

	nodes_pom := []Node{*leaf1, *leaf2, *leaf3}

	//Ovo takodje odkomentarisemo kako bismo radili sa ulaznim podacima
	//root_node := makeMerkel(nodes)
	root_node := MakeMerkel(nodes_pom)

	WriteFile("metadata.txt", *root_node.root)

	fmt.Println("Ovo je test funkcije : ", root_node.String())

	// Ovde proveravamo jer ovi ispod izrazi daju tacno resenje

	leaf1.hash = Hash(leaf1.data)
	leaf2.hash = Hash(leaf2.data)
	leaf3.hash = Hash(leaf3.data)

	parent1 := &Node{left: leaf1, right: leaf2, hash: Hash(append(leaf1.data[:], leaf2.data[:]...))}
	parent2 := &Node{left: leaf3, hash: Hash(leaf3.data[:])}

	mr.root = &Node{left: parent1, right: parent2, hash: Hash(append(parent1.data[:], parent2.data[:]...))}

	fmt.Println("\nVrednost leaf1 node-a: ", leaf1.Data_String())
	fmt.Println("\nHash root node-a: ", mr.String())

}
