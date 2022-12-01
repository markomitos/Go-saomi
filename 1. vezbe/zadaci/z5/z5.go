package main

import "fmt"

type Student struct {
	Ime, Prezime, Index string
}

func (s *Student) IzmenaIndexa(index string) {
	s.Index = index
}

func main() {
	marko := Student{"Marko", "Mitosevic", "SV56/21"}
	fmt.Println(marko)
	marko.IzmenaIndexa("SV57/21")
	fmt.Println(marko)
}
