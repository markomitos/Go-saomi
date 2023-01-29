package cms

import (
	"math"
)

type CountMinSketch struct {
	k          uint    //broj hashFunkcija (dubina)
	m          uint    //broj kolona (sirina)
	epsilon    float64 //Preciznost
	delta      float64 //Sigutnost tacnosti (falsePositive rate)
	valueTable [][]uint
	hashFuncs  []HashWithSeed
}

func NewCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	cms := new(CountMinSketch)
	cms.k = CalculateK(delta)
	cms.m = CalculateM(epsilon)
	cms.epsilon = epsilon
	cms.delta = delta
	cms.hashFuncs = CreateHashFunctions(cms.k)
	cms.valueTable = make([][]uint, cms.k)
	for i := range cms.valueTable {
		cms.valueTable[i] = make([]uint, cms.m)
	}
	return cms
}

func AddToCms(cms *CountMinSketch, elem []byte) {
	for i, fn := range cms.hashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.m)))
		cms.valueTable[i][hashedValue]++
	}
}

func CheckFrequencyInCms(cms *CountMinSketch, elem []byte) uint {
	//Pomocni slice pomocu kojeg racunam min (sastoji se od svih vrednosti)
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
