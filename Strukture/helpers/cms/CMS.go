package cms

import (
	"math"
)

type CountMinSketch struct {
	K          uint32    //broj hashFunkcija (dubina)
	M          uint32    //broj kolona (sirina)
	Epsilon    float64 //Preciznost
	Delta      float64 //Sigutnost tacnosti (falsePositive rate)
	ValueTable [][]uint32
	HashFuncs  []CmsHashWithSeed
}

func NewCountMinSketch(Epsilon float64, Delta float64) *CountMinSketch {
	cms := new(CountMinSketch)
	cms.K = CmsCalculateK(Delta)
	cms.M = CmsCalculateM(Epsilon)
	cms.Epsilon = Epsilon
	cms.Delta = Delta
	cms.HashFuncs = CmsCreateHashFunctions(cms.K)
	cms.ValueTable = make([][]uint32, cms.K)
	for i := range cms.ValueTable {
		cms.ValueTable[i] = make([]uint32, cms.M)
	}
	return cms
}

func AddToCms(cms *CountMinSketch, elem []byte) {
	for i, fn := range cms.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.M)))
		cms.ValueTable[i][hashedValue]++
	}
}

func CheckFrequencyInCms(cms *CountMinSketch, elem []byte) uint32 {
	//Pomocni slice pomocu kojeg racunam min (sastoji se od svih vrednosti)
	arr := make([]uint32, cms.K)
	for i, fn := range cms.HashFuncs {
		hashedValue := int(math.Mod(float64(fn.Hash(elem)), float64(cms.M)))
		arr[i] = cms.ValueTable[i][hashedValue]
	}
	min := arr[0]
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}
