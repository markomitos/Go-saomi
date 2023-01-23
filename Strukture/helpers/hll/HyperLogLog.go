package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"strconv"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

// Hash i pretvaranje u binaran oblik
func Hash(data string) string {
	// fn := md5.New()
	// num := binary.BigEndian.Uint32(fn.Sum(data))
	h := fnv.New32a()
	h.Write([]byte(data))
	num := h.Sum32()
	fmt.Println(num)

	//Dodajemo nule na pocetak da se dopune 32 bita
	str := fmt.Sprintf("%b", num)
	for len(str) < 32 {
		str = "0" + str
	}
	fmt.Println(str)
	return str
}

// Konstruktor
func newHLL(precision uint8) (*HLL, error) {
	hll := new(HLL)
	if precision < HLL_MIN_PRECISION && precision > HLL_MAX_PRECISION {
		return nil, errors.New("Preciznost mora biti izmedju 4 i 16")
	}
	hll.p = precision
	hll.m = uint64(math.Pow(2, float64(precision)))
	hll.reg = make([]uint8, hll.m)
	return hll, nil
}

func (hll *HLL) addToHLL(elem string) {
	hashString := Hash(elem)
	fmt.Println(hashString)
	bucketString := hashString[:hll.p]
	bucket, err := strconv.ParseInt(bucketString, 2, 64)
	if err != nil {
		log.Fatal(err)
	}

	zerosCount := 1
	for i := uint8(len(hashString)) - 1; i >= hll.p; i-- {
		if hashString[i] == '0' {
			zerosCount++
		} else {
			break
		}
	}

	if hll.reg[bucket] < uint8(zerosCount) {
		hll.reg[bucket] = uint8(zerosCount)
	}

}

// Vraca procenjenu kardinalnost
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

// Pomocna funkcija koja racuna nule
func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
