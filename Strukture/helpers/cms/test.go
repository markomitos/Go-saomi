package cms

import (
	"fmt"
)

func main() {
	cmsA := NewCountMinSketch(0.1, 0.9)
	AddToCms(cmsA, []byte("majmun"))
	AddToCms(cmsA, []byte("majmun"))
	AddToCms(cmsA, []byte("majmun"))
	AddToCms(cmsA, []byte("banana"))
	AddToCms(cmsA, []byte("drvo"))
	AddToCms(cmsA, []byte("kokos"))
	AddToCms(cmsA, []byte("kokos"))

	fmt.Println(CheckFrequencyInCms(cmsA, []byte("majmun")))
	fmt.Println(CheckFrequencyInCms(cmsA, []byte("drvo")))

}
