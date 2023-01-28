package sstable

type Index struct {
	offset uint64
	key    string
}

type Summary struct {
	firstKey string
	lastKey  string
	offset   uint64
}

type SSTable struct {
	key []string
}
