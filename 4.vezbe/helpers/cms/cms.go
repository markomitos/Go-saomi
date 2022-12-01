package main

import "math"

type CountMinSketch struct {
	k          uint
	m          uint
	epsilon    float64
	delta      float64
	valueTable [][]uint
	hashFuncs  []HashWithSeed
}

func newCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	cms := new(CountMinSketch)
	cms.k = CalculateK(delta)
	cms.m = CalculateM(epsilon)
	cms.delta = delta
	cms.epsilon = epsilon
	cms.valueTable = make([][]uint, cms.k)
	for i, _ := range cms.valueTable {
		cms.valueTable[i] = make([]uint, cms.m)
	}
	cms.hashFuncs = CreateHashFunctions(cms.k)
	return cms
}

func addToCms(cms *CountMinSketch, elem []byte) {
	for i, fn := range cms.hashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.m)))
		cms.valueTable[i][hashedValue]++
	}
}

func checkFrequencyInCms(cms *CountMinSketch, elem []byte) uint {
	arr := make([]uint, cms.k)
	for i, fn := range cms.hashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.m)))
		arr[i] = cms.valueTable[i][hashedValue]
	}
	min := arr[0]
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}
