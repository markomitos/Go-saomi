package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	//stringovi posle atributa su tu da bi Unmarshal znao gde sta da namapira
	WalSize           int    `yaml:"wal_size"`
	MemtableSize      int    `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
}

func (c *Config) Configure() {
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	//upisuje sve iz fileu u osobine configu
	yaml.Unmarshal(configData, c)
}

func main() {
	c := new(Config)
	c.Configure()
	print("nesto")
}
