package main

import "math"

type BloomFilter struct {
	bitset   []bool
	k        uint
	n        uint
	m        uint
	hashFunc []HashWithSeed
}

func newBloomFilter(expectedNumOfElem int, falsePositiveRates float64) *BloomFilter {
	b := new(BloomFilter)
	b.m = CalculateM(expectedNumOfElem, falsePositiveRates)
	b.k = CalculateK(expectedNumOfElem, b.m)
	b.hashFunc = CreateHashFunctions(b.k)
	b.bitset = make([]bool, b.m)
	b.n = 0
	return b
}

func addToBloom(b *BloomFilter, elem []byte) {
	b.n++
	for _, fn := range b.hashFunc {
		hashValue := math.Mod(float64(fn.Hash(elem)), float64(b.m))
		b.bitset[int(hashValue)] = true
	}
}

func isInBloom(b *BloomFilter, elem []byte) bool {
	for _, fn := range b.hashFunc {
		hashValue := math.Mod(float64(fn.Hash(elem)), float64(b.m))
		if b.bitset[int(hashValue)] == false {
			return false
		}
	}
	return true
}
