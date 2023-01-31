package main

import (
	"fmt"
	"time"
)

type TokenBucket struct {
	rate     int       // Tokena po sekundi stize u baket
	capacity int       // Kapacitet tokena
	tokens   int       // Trenutno tokena
	last     time.Time // Poslednji zahtev ili inicijalizacija
	lock     chan struct{}
}

func NewTokenBucket(rate, capacity int) *TokenBucket {
	return &TokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
		last:     time.Now(),
		lock:     make(chan struct{}, 1),
	}
}

func (b *TokenBucket) Take(tokens int) bool {
	b.lock <- struct{}{}
	// Sluzi za resavanje race conditiona
	// Znaci da sprecava istovremene goroutine odnosno threadove
	// Zbog paralelnog pristupa kako se ne bismo oslanjali na brzinu izvrsavanja operacija
	// Sto moze prouzrokovati dodatne bugove koje je tesko debugovati
	defer func() {
		<-b.lock
	}()

	// Proverava koliko je sekundi proslo od poslednjeg pristupa
	// Toliko popunjava bucket tokenima po nase rate-u, ako prekoracuje kapacitet
	// Samo ostaje popunjen maksimalno
	now := time.Now()
	d := now.Sub(b.last).Seconds()
	b.tokens += int(float64(b.rate) * d)
	b.last = now

	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}

	if b.tokens < tokens {
		return false
	}
	b.tokens -= tokens
	return true
}

func main() {
	bucket := NewTokenBucket(10, 10)
	for i := 0; i < 200; i++ {
		if bucket.Take(1) {
			fmt.Println("Zahtev odobren")
		} else {
			fmt.Println("Zahtev neodobren")
		}
		// Saljemo req svakih 0.01 sekundi
		time.Sleep(time.Millisecond * 10)
	}
}
