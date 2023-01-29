package dataType

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
