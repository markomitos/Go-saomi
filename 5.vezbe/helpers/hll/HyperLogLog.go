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

func newHLL(precision uint8) (*HLL, error) {
	h := new(HLL)
	if !(precision >= HLL_MIN_PRECISION && precision <= HLL_MAX_PRECISION) {
		return nil, errors.New("Preciznost mora biti izmedju 4 i 16")
	}
	h.p = precision
	h.m = uint64(math.Pow(2, float64(h.p)))
	reg := make([]byte, h.m)
	h.reg = reg
	return h, nil
}

func Hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	str := fmt.Sprintf("%b", h.Sum32())
	for len(str) < 32 {
		str = "0" + str
	}
	return str
}

func (h *HLL) addToHLL(elem string) {
	hash := Hash(elem)
	var skey string = hash[:h.p]
	key, err := strconv.ParseInt(skey, 2, 64)
	if err != nil {
		log.Fatal(err)
	}
	var counter uint8 = 1
	for i := uint8(len(hash) - 1); i >= h.p-1; i-- {
		if hash[i] == '0' {
			counter++
		} else {
			break
		}
	}
	if h.reg[key] < uint8(counter) {
		h.reg[key] = counter
	}
}

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

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
