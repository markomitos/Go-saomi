package main

import (
	"example/cms/cms"
	"fmt"
)

func main() {
	cmsA := cms.NewCountMinSketch(0.1, 0.9)
	cms.AddToCms(cmsA, []byte("majmun"))
	cms.AddToCms(cmsA, []byte("majmun"))
	cms.AddToCms(cmsA, []byte("majmun"))
	cms.AddToCms(cmsA, []byte("banana"))
	cms.AddToCms(cmsA, []byte("drvo"))
	cms.AddToCms(cmsA, []byte("kokos"))
	cms.AddToCms(cmsA, []byte("kokos"))

	fmt.Println(cms.CheckFrequencyInCms(cmsA, []byte("majmun")))
	fmt.Println(cms.CheckFrequencyInCms(cmsA, []byte("drvo")))

}
