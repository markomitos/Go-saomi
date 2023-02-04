package dataType

import "fmt"

type Data struct {
	Value     []byte
	Tombstone bool
	Timestamp uint64
}

func NewData(val []byte, tombstone bool, timestamp uint64) *Data {
	data := new(Data)
	data.Value = val
	data.Tombstone = tombstone
	data.Timestamp = timestamp
	return data
}

func (data *Data) Print() {
	fmt.Println(" ------------ DATA ------------")
	fmt.Println(data.Value)
	fmt.Println(data.Tombstone)
	fmt.Println(data.Timestamp)
}