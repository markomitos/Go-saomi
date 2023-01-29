package least_reacently_used

import "testing"

func TestCache(t *testing.T) {
	cache := NewLRU(1)
	cache.Set("srbija", "beograd")
	u := cache.Get("srbija")
	if u != "beograd" {
		t.Fatalf("Ocekivano 'beograd', dobijeno '%s'", u)
	}

	cache.Set("uk", "manchester")
	u = cache.Get("uk")
	if u != "manchester" {
		t.Fatalf("Expected 'manchester', got '%s'", u)
	}
}

func TestLRU(t *testing.T) {
	cache := NewLRU(3)
	cache.Set("uk", "london")
	cache.Set("france", "beograd")
	cache.Set("germany", "mongol")

	cache.Set("belgium", "brussels")

	u := cache.Get("uk")
	if u != "" {
		t.Fatalf("uk should no longer be preasent")
	}

	// belgium mongolia serbia

	f := cache.Get("france")

	if f != "beograd" {
		t.Fatalf("expected 'paris', but got %s", f)
	}
	// red : france - belgium - germany

	cache.Set("netherlands", "amsterdam")

	// red : netherlands - france - belgium
}
