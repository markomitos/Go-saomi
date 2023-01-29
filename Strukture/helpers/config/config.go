package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

// var Configuration *Config //GLOBALAN CONFIG

// Default vrednosti
const default_WalSize = 10
const default_MemtableSize = 10
const default_MemtableStructure = "skiplist"
const default_SStableInterval = 128
const default_BloomFalsePositiveRate = 2.0
const default_BTreeNumOfChildren = 3

type Config struct {
	//stringovi posle atributa su tu da bi Unmarshal znao gde sta da namapira
	WalSize                int     `yaml:"wal_size"`
	MemtableSize           int     `yaml:"memtable_size"`
	MemtableStructure      string  `yaml:"memtable_structure"`
	SStableInterval        uint    `yaml:"sstable_interval"`
	BloomFalsePositiveRate float64 `yaml:"bloom_falsepositive_rate"`
	BTreeNumOfChildren     uint    `yaml:"btree_num_of_children"`
}

// Ukoliko unutar config.yml fali neki atribut
func initializeConfig() *Config {
	c := new(Config)
	c.WalSize = default_WalSize
	c.MemtableSize = default_MemtableSize
	c.MemtableStructure = default_MemtableStructure
	c.SStableInterval = default_SStableInterval
	c.BloomFalsePositiveRate = default_BloomFalsePositiveRate
	c.BTreeNumOfChildren = default_BTreeNumOfChildren
	return c
}

func GetConfig() *Config {
	c := initializeConfig()

	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	//upisuje sve iz fileu u osobine configu
	yaml.Unmarshal(configData, c)

	// Provera defaultnih vrednosti
	if c.WalSize == 0 {
		c.WalSize = default_WalSize
	}

	if c.MemtableSize == 0 {
		c.MemtableSize = default_MemtableSize
	}

	if c.MemtableStructure == "" {
		c.MemtableStructure = default_MemtableStructure
	}

	if c.SStableInterval == 0 {
		c.SStableInterval = default_SStableInterval
	}

	if c.BloomFalsePositiveRate == 0.0 {
		c.BloomFalsePositiveRate = default_BloomFalsePositiveRate
	}

	if c.BTreeNumOfChildren == 0 {

	}

	return c
}
