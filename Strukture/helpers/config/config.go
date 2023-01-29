package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

var Configuration *Config //GLOBALAN CONFIG

// Default vrednosti
const default_WalSize = 10
const default_MemtableSize = 10
const default_MemtableStructure = "skiplist"

type Config struct {
	//stringovi posle atributa su tu da bi Unmarshal znao gde sta da namapira
	WalSize           int    `yaml:"wal_size"`
	MemtableSize      int    `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
}

func LoadConfig() {
	c := new(Config)

	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	//upisuje sve iz fileu u osobine configu
	yaml.Unmarshal(configData, c)

	if c.WalSize == 0 {
		c.WalSize = default_WalSize
	}

	if c.MemtableSize == 0 {
		c.MemtableSize = default_MemtableSize
	}

	if c.MemtableStructure == "" {
		c.MemtableStructure = default_MemtableStructure
	}

	Configuration = c
}

// func main() {
// 	c := new(Config)
// 	c.Configure()
// 	print(c)
// }
