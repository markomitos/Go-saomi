package least_reacently_used

import (
	"container/list"
	"project/gosaomi/config"
)

// Koristicemo mapu i linked listu za LRU

type LRUCache struct {
	m   map[string]*cacheMapElement
	cap int
	l   list.List
}

type cacheMapElement struct {
	el    *list.Element
	value string
}

func NewLRU() LRUCache {
	c := config.GetConfig()

	return LRUCache{
		m:   map[string]*cacheMapElement{},
		cap: c.LruCap,
		l:   list.List{},
	}
}

func (c *LRUCache) Get(key string) string {
	v, ok := c.m[key]
	if !ok {
		return ""
	}
	c.l.MoveToFront(v.el)
	return v.value
}

func (c *LRUCache) Set(key string, data *Data) {
	v, ok := c.m[key]
	if !ok {
		el := c.l.PushFront(key)
		c.m[key] = &cacheMapElement{
			el:    el,
			value: value,
		}

		if c.l.Len() > c.cap {
			backEl := c.l.Back()
			backElementKey := backEl.Value.(string)
			c.l.Remove(backEl)
			delete(c.m, backElementKey)
		}

	} else {
		v.value = value
		c.l.MoveToFront(v.el)
	}

}
