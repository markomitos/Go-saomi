package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

// Default vrednosti
const default_WalBufferCapacity = 10
const default_WalWaterMark = 20
const default_MemtableSize = 10
const default_MemtableStructure = "b_tree"
const default_SStableInterval = 128
const default_BloomFalsePositiveRate = 2.0
const default_BTreeNumOfChildren = 3
const default_SSTableFileConfig = "single"
const default_LsmMaxLevel = 4
const default_TokenBucketCap = 25
const default_TokenBucketRate = 15
const default_CompactionType = "size_tiered"

type Config struct {
	//stringovi posle atributa su tu da bi Unmarshal znao gde sta da namapira
	WalBufferCapacity                int     `yaml:"wal_buffer_capacity"`
	WalWaterMark uint `yaml:"wal_water_mark"`
	MemtableSize           uint    `yaml:"memtable_size"`
	MemtableStructure      string  `yaml:"memtable_structure"`
	SStableInterval        uint    `yaml:"sstable_interval"`
	BloomFalsePositiveRate float64 `yaml:"bloom_falsepositive_rate"`
	BTreeNumOfChildren     uint    `yaml:"btree_num_of_children"`
	SSTableFileConfig      string  `yaml:"sstable_file_config"`
	LsmMaxLevel            uint    `yaml:"lsm_max_level"`
	TokenBucketCap         int     `yaml:"token_cap"`
	TokenBucketRate        int     `yaml:"token_rate"`
	CompactionType string `yaml:"compaction_type"`
}

// Ukoliko unutar config.yml fali neki atribut
func initializeConfig() *Config {
	c := new(Config)
	c.WalBufferCapacity = default_WalBufferCapacity
	c.WalWaterMark = default_WalWaterMark
	c.MemtableSize = default_MemtableSize
	c.MemtableStructure = default_MemtableStructure
	c.SStableInterval = default_SStableInterval
	c.BloomFalsePositiveRate = default_BloomFalsePositiveRate
	c.BTreeNumOfChildren = default_BTreeNumOfChildren
	c.SSTableFileConfig = default_SSTableFileConfig
	c.LsmMaxLevel = default_LsmMaxLevel
	c.TokenBucketCap = default_TokenBucketCap
	c.TokenBucketRate = default_TokenBucketRate
	c.CompactionType = default_CompactionType
	return c
}

// Dobavlja konfiguraciju iz fajla
func GetConfig() *Config {
	c := initializeConfig()

	configData, err := ioutil.ReadFile("config/config.yml")
	if err != nil {
		log.Fatal(err)
	}
	//upisuje sve iz fileu u osobine configu
	yaml.Unmarshal(configData, c)

	// Provera defaultnih vrednosti
	if c.WalBufferCapacity < 2 {
		c.WalBufferCapacity = default_WalBufferCapacity
	}

	if c.WalWaterMark < 10 {
		c.WalWaterMark = default_WalWaterMark
	}

	if c.MemtableSize == 0 {
		c.MemtableSize = default_MemtableSize
	}

	if c.MemtableStructure != "skiplist" && c.MemtableStructure != "b_tree" {
		c.MemtableStructure = default_MemtableStructure
	}

	if c.SStableInterval == 0 {
		c.SStableInterval = default_SStableInterval
	}

	if c.BloomFalsePositiveRate == 0.0 {
		c.BloomFalsePositiveRate = default_BloomFalsePositiveRate
	}

	if c.BTreeNumOfChildren == 0 {
		c.BTreeNumOfChildren = default_BTreeNumOfChildren
	}

	if c.SSTableFileConfig != "single" && c.SSTableFileConfig != "multiple" {
		c.SSTableFileConfig = default_SSTableFileConfig
	}

	if c.LsmMaxLevel < 4 {
		c.LsmMaxLevel = default_LsmMaxLevel
	}

	if c.TokenBucketCap == 0 {
		c.TokenBucketCap = default_TokenBucketCap
	}

	if c.TokenBucketRate == 0 {
		c.TokenBucketRate = default_TokenBucketRate
	}

	if c.CompactionType != "size_tiered" && c.CompactionType != "leveled"{
		c.CompactionType = default_CompactionType
	}

	return c
}
