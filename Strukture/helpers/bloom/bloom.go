package bloom

import (
	"math"
)

type BloomFilter struct {
	Bitset    []bool // niz
	K         uint32 // Number of hash values
	N         uint32 // Number of elements in the filter
	M         uint32 // Size of the bloom filter bitset
	HashFuncs []HashWithSeed
}

// Konstruktor
func NewBloomFilter(expectedNumOfElem uint32, falsePositiveRate float64) *BloomFilter {
	blm := new(BloomFilter)
	blm.M = CalculateM(expectedNumOfElem, falsePositiveRate)
	blm.K = CalculateK(expectedNumOfElem, blm.M)
	blm.N = 0
	blm.HashFuncs = CreateHashFunctions(blm.K)
	blm.Bitset = make([]bool, blm.M)
	return blm
}

func (blm *BloomFilter) AddToBloom(elem []byte) {
	blm.N++
	for _, fn := range blm.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.M)))
		blm.Bitset[hashedValue] = true
	}
}

func (blm *BloomFilter) IsInBloom(elem []byte) bool {
	for _, fn := range blm.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.M)))
		if blm.Bitset[hashedValue] == false {
			return false
		}
	}
	return true
}
