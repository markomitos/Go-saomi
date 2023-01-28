package least_reacently_used

import (
	"container/list"
	"fmt"
)

// Koristicemo mapu i linked listu za LRU

type Cache interface {
	Get(key string) string
	Set(key, value string)
}

type LRUCache struct {
	m   map[string]string
	cap int
	l   list.List
}

func NewLRU(cap int) LRUCache {
	return LRUCache{
		m:   map[string]string{},
		cap: cap,
		l:   list.List{},
	}
}

func (l *LRUCache) Get(key string) string {
	return l.m[key]
}
func (l *LRUCache) Set(key, value string) {
	l.m[key] = value
}

func least_reacently_used() {

	cache := NewLRU(1)
	cache.Set("srbija", "beograd")
	u := cache.Get("srbija")
	if u != "beograd" {
		fmt.Println("Ocekivano 'beograd', dobijeno ")
	}
}
