package scan

import (
	. "project/gosaomi/dataType"
)

// FoundResults -> trenutan broj pronadjenih poklapanja uslova
// SelectedPageStart -> od kog pronadjenog rezultata treba da belezi
// SelectedPageEnd -> do kog pronadjenog rezultata treba da belezi
// Keys -> niz kljuceva koji cemo azurirati kako god pronadjemo rezultat u opsegu
// Data -> niz podataka koji cemo azurirati kako god pronadjemo rezultat u opsegu

//Struktura koja predstavlja trenutnu potragu za stranicom
type Scan struct {
	FoundResults uint32
	SelectedPageStart uint32
	SelectedPageEnd uint32
	Keys []string
	Data []*Data
}

func NewScan(pageLen uint32, pageNum uint32) *Scan{
	scan := new(Scan)
	scan.FoundResults = 0
	scan.Data = make([]*Data, 0)
	scan.Keys = make([]string, 0)
	scan.SelectedPageStart = (pageNum-1)*pageLen+1
	scan.SelectedPageEnd = (pageNum)*pageLen
	return scan
}