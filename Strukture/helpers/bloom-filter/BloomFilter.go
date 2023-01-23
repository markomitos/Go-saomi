package main

import (
	"math"
)

type BloomFilter struct {
	bitset    []bool // niz
	k         uint   // Number of hash values
	n         uint   // Number of elements in the filter
	m         uint   // Size of the bloom filter bitset
	hashFuncs []HashWithSeed
}

// Konstruktor
func newBloomFilter(expectedNumOfElem int, falsePositiveRate float64) *BloomFilter {
	blm := new(BloomFilter)
	blm.m = CalculateM(expectedNumOfElem, falsePositiveRate)
	blm.k = CalculateK(expectedNumOfElem, blm.m)
	blm.n = 0
	blm.hashFuncs = CreateHashFunctions(blm.k)
	blm.bitset = make([]bool, blm.m)
	return blm
}

func addToBloom(blm *BloomFilter, elem []byte) {
	blm.n++
	for _, fn := range blm.hashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.m)))
		blm.bitset[hashedValue] = true
	}
}

func isInBloom(blm *BloomFilter, elem []byte) bool {
	for _, fn := range blm.hashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(blm.m)))
		if blm.bitset[hashedValue] == false {
			return false
		}
	}
	return true
}
