package main

import (
	"flag"
	"github.com/mongodbinc-interns/mongoproxy"
	"github.com/mongodbinc-interns/mongoproxy/log"
	"gopkg.in/mgo.v2/bson"
)

const DEFAULT_PORT int = 8124
const DEFAULT_CONFIG_URI string = "mongodb://localhost:27017"
const DEFAULT_CONFIG_NS string = "test.config"

var (
	port            int
	logLevel        int
	mongoURI        string
	configNamespace string
	configFilename  string
)

func parseFlags() {
	flag.IntVar(&port, "port", DEFAULT_PORT, "Port to listen on")
	flag.IntVar(&logLevel, "logLevel", log.NOTICE, "Verbosity for logging")
	flag.StringVar(
		&mongoURI,
		"m",
		DEFAULT_CONFIG_URI,
		"MongoDB instance to connect to for configuration",
	)
	flag.StringVar(
		&configNamespace,
		"c",
		DEFAULT_CONFIG_NS,
		"MongoDB namespace to query for configuration.",
	)
	flag.StringVar(&configFilename, "f", "",
		"Config filename. If set, will be used instead of MongoDB.")
	flag.Parse()
}

func main() {

	parseFlags()
	log.SetLogLevel(logLevel)

	// grab config file
	var result bson.M
	var err error
	if len(configFilename) > 0 {
		result, err = mongoproxy.ParseConfigFromFile(configFilename)
	} else if len(configNamespace) > 0 {
		log.Log(log.INFO, "namespace=%s", configNamespace)
		result, err = mongoproxy.ParseConfigFromDB(mongoURI, configNamespace)
	} else {
		log.Log(log.ERROR, "Need either a DB namespace or filename for config")
	}

	if err != nil {
		log.Log(log.WARNING, "%v", err)
	}

	mongoproxy.StartWithConfig(port, result)
}
