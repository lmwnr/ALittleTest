package conf

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// parse from conf.json
type confjson struct {
	AWSKeyId     string
	AWSSecretKey string
	PgConnRW     string
	PgConnRO     string
}

// convert from confjson
type base struct {
	confjson
}

const (
	confFile = "./conf/conf.json"
)

var (
	D base
)

func init() {
	cfile, err := ioutil.ReadFile(confFile)
	if err != nil {
		panic(fmt.Errorf("fail to load configuration file %s", err))
	}

	var cjson confjson
	if err := json.Unmarshal(cfile, &cjson); err != nil {
		panic(err)
	}

	D = base{confjson: cjson}
}
