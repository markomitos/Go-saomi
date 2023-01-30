package sstable

import (
	. "project/gosaomi/config"
	. "project/gosaomi/dataType"
)

type SST interface {
	makeFiles()
	Flush(keys []string, values []*Data)
	ReadData()
	ReadIndex()
	ReadSummary()
	ReadBloom()
	Find(key string)
}

type Index struct {
	Offset  uint64
	KeySize uint32 //Ovo cuva velicinu kljuca i pomocu toga citamo iz fajla
	Key     string
}

type Summary struct {
	FirstKey  string
	LastKey   string
	Intervals []*Index
}

func NewSSTable(size uint32, directory string) *SST {
	config := GetConfig()
	var sstable SST
	if config.SSTableFileConfig == "single"{
		sstable = NewSSTableSingle(size, directory)
	} else {
		sstable = NewSSTableMulti(size, directory)
	}

	return sstable
}