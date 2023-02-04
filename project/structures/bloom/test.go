package bloom

import (
	"fmt"
)

func main() {
	blm := NewBloomFilter(10, 2)
	blm.AddToBloom([]byte("MAJMUN"))
	blm.AddToBloom([]byte("filadendron"))
	fmt.Println(blm.IsInBloom([]byte("faradon")))
}
