package least_reacently_used

import (
	"container/list"
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"project/gosaomi/config"
	. "project/gosaomi/dataType"
	. "project/gosaomi/entry"
)

// Koristicemo mapu i linked listu za LRU

type LRUCache struct {
	elementMap map[string]*cacheMapElement
	cap        int
	keyList    list.List
}

type cacheMapElement struct {
	el    *list.Element
	value *Data
}

func (lru *LRUCache) Delete(key string) {
	lru.keyList.Remove(lru.elementMap[key].el)
	delete(lru.elementMap, key)
	lru.Write()
}

func NewLRU() *LRUCache {
	c := config.GetConfig()

	return &LRUCache{
		elementMap: map[string]*cacheMapElement{},
		cap:        c.LruCap,
		keyList:    list.List{},
	}
}

func (lru *LRUCache) Write() {
	//Trazimo lokaciju fajla
	path, err1 := filepath.Abs("files/cache/cache.bin")
	if err1 != nil {
		log.Fatal(err1)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil {
		if os.IsNotExist(err) {
			file, err1 = os.Create(path)
			if err1 != nil {
				log.Fatal(err1)
			}
		} else {
			log.Fatal(err)
		}
	}
	err = file.Truncate(0)
	if err != nil {
		return
	} //Brise ga
	// Prolazak kroz dvostruko spregnutu listu
	for e := lru.keyList.Front(); e != nil; e = e.Next() {
		//zapisujemo entry kao niz bytova
		key := e.Value.(string)
		entry := NewEntry(key, lru.elementMap[key].value)

		_, err = file.Write(EntryToBytes(entry))
		if err != nil {
			log.Fatal(err)
		}
	}
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func ReadLru() *LRUCache {
	lru := NewLRU()
	// Otvaramo fajl
	file, err := os.OpenFile("files/cache/cache.bin", os.O_RDONLY, 0777)
	if err != nil {
		if os.IsNotExist(err) {
			path, err1 := filepath.Abs("files/cache/cache.bin")
			if err1 != nil {
				log.Fatal(err1)
			}

			file, err1 = os.Create(path)
			if err1 != nil {
				log.Fatal(err1)
			}
			return lru
		} else {
			log.Fatal(err)
		}
	}

	// Citamo slogove
	for i := 0; i < lru.cap; i++ {
		entry := ReadEntry(file)
		if entry == nil {
			break
		}

		lru.keyList.PushBack(string(entry.Key))
		timestamp := binary.BigEndian.Uint64(entry.Timestamp)
		tombstone := entry.Tombstone[0] == uint8(1)

		data := NewData(entry.Value, tombstone, timestamp)

		cache := new(cacheMapElement)
		cache.el = lru.keyList.Back()
		cache.value = data

		lru.elementMap[string(entry.Key)] = cache
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	return lru
}

func (lru *LRUCache) Get(key string) (bool, *Data) {
	elem, ok := lru.elementMap[key]
	if !ok {
		return false, nil
	}
	lru.keyList.MoveToFront(elem.el)
	lru.Write()
	return true, elem.value
}

func (lru *LRUCache) Set(key string, value *Data) {
	v, ok := lru.elementMap[key]
	if !ok {
		el := lru.keyList.PushFront(key)
		lru.elementMap[key] = &cacheMapElement{
			el:    el,
			value: value,
		}

		if lru.keyList.Len() > lru.cap {
			backEl := lru.keyList.Back()
			backElementKey := backEl.Value.(string)
			lru.keyList.Remove(backEl)
			delete(lru.elementMap, backElementKey)
		}

	} else {
		v.value = value
		lru.keyList.MoveToFront(v.el)
	}
	lru.Write()
}

//
//func main() {
//	lru := NewLRU()
//	lru.Set("1", NewData([]byte("ognjen"), false, uint64(time.Now().Unix())))
//	lru.Set("2", NewData([]byte("vesna"), false, uint64(time.Now().Unix())))
//	lru.Set("3", NewData([]byte("ilija"), false, uint64(time.Now().Unix())))
//	lru.Set("4", NewData([]byte("branko"), false, uint64(time.Now().Unix())))
//	lru.Set("5", NewData([]byte("marko"), false, uint64(time.Now().Unix())))
//	lru.Set("6", NewData([]byte("zarko"), false, uint64(time.Now().Unix())))
//
//	found, elem := lru.Get("4")
//
//	if found {
//		println("super pronadjen: ", string(elem.Value))
//	}
//
//	println("sada brisemo")
//	lru.Delete("4")
//
//	found, elem = lru.Get("4")
//
//	if !found {
//		println("Sve super nije pronadjen element koji je obrisan")
//	}
//
//	//
//	//lru.WriteLru()
//	//ime := lru.Get("2")
//	//fmt.Println(ime)
//	//lru.Set("7", "darko")
//	//ime = lru.Get("2")
//	//fmt.Println(ime)
//}
