package least_reacently_used

import (
	"container/list"
	"encoding/binary"
	"log"
	"os"
	"project/gosaomi/config"
	"project/gosaomi/dataType"
	. "project/gosaomi/entry"
)

// Koristicemo mapu i linked listu za LRU

//type Cache interface {
//	Get(key string) string
//	Set(key, value string)
//}

type LRUCache struct {
	m   map[string]*cacheMapElement
	cap int
	l   list.List
}

type cacheMapElement struct {
	el    *list.Element
	value dataType.Data
}

func NewLRU() LRUCache {
	c := config.GetConfig()

	return LRUCache{
		m:   map[string]*cacheMapElement{},
		cap: c.LruCap,
		l:   list.List{},
	}
}

func (c *LRUCache) WriteLru() bool {
	file, err := os.OpenFile("files/cache/cache.bin", os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
		return false
	}
	// Prolazak kroz dvostruko spregnutu listu
	for e := c.l.Front(); e != nil; e = e.Next() {
		//zapisujemo entry kao niz bytova
		key := e.Value.(string)
		entry := NewEntry(key, &c.m[key].value)

		_, err = file.Write(EntryToBytes(entry))
		if err != nil {
			log.Fatal(err)
			return false
		}
	}
	err = file.Close()
	if err != nil {
		return false
	}
	return true
}

func (c *LRUCache) ReadLru() bool {
	// Otvaramo fajl
	file, err := os.OpenFile("files/cache/cache.bin", os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
		return false
	}
	// Citamo slogove

	for i := 0; i < c.cap; i++ {

		entry := ReadEntry(file)

		c.l.PushBack(string(entry.Key))
		timestamp := binary.BigEndian.Uint64(entry.Timestamp)
		tombstone := entry.Tombstone[0] == uint8(1)

		data := dataType.Data{
			Value:     entry.Value,
			Tombstone: tombstone,
			Timestamp: timestamp,
		}

		cache := new(cacheMapElement)
		cache.el = c.l.Back()
		cache.value = data

		c.m[string(entry.Key)] = cache
	}

	err = file.Close()
	if err != nil {
		return false
	}

	return true
}

func (c *LRUCache) Get(key string) dataType.Data {
	v, ok := c.m[key]
	if !ok {
		return dataType.Data{}
	}
	c.l.MoveToFront(v.el)
	return v.value
}

func (c *LRUCache) Set(key string, value *dataType.Data) {
	v, ok := c.m[key]
	if !ok {
		el := c.l.PushFront(key)
		c.m[key] = &cacheMapElement{
			el:    el,
			value: *value,
		}

		if c.l.Len() > c.cap {
			backEl := c.l.Back()
			backElementKey := backEl.Value.(string)
			c.l.Remove(backEl)
			delete(c.m, backElementKey)
		}

	} else {
		v.value = *value
		c.l.MoveToFront(v.el)
	}

}

//func main() {
//	lru := NewLRU()
//	lru.Set("1", dataType.NewData([]byte("ognjen"), false, uint64(time.Now().Unix())))
//	lru.Set("2", dataType.NewData([]byte("vesna"), false, uint64(time.Now().Unix())))
//	lru.Set("3", dataType.NewData([]byte("ilija"), false, uint64(time.Now().Unix())))
//	lru.Set("4", dataType.NewData([]byte("branko"), false, uint64(time.Now().Unix())))
//	lru.Set("5", dataType.NewData([]byte("marko"), false, uint64(time.Now().Unix())))
//	lru.Set("6", dataType.NewData([]byte("zarko"), false, uint64(time.Now().Unix())))
//	//
//	lru.WriteLru()
//	//ime := lru.Get("2")
//	//fmt.Println(ime)
//	//lru.Set("7", "darko")
//	//ime = lru.Get("2")
//	//fmt.Println(ime)
//
//}
