package main

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Alert struct {
	File      string   `yaml:"file"`
	Ignoreora []string `yaml:"ignoreora"`
}

type Query struct {
	Sql     string   `yaml:"sql"`
	Name    string   `yaml:"name"`
	Metrics []string `yaml:"metrics"`
	Labels  []string `yaml:"labels"`
	Help    string   `yaml:"help"`
}

type Config struct {
	Connection string  `yaml:"connection"`
	Database   string  `yaml:"database"`
	Instance   string  `yaml:"instance"`
	Alertlog   []Alert `yaml:"alertlog"`
	Queries    []Query `yaml:"queries"`
	db         *sql.DB
}

type Configs struct {
	Cfgs []Config `yaml:"connections"`
}

var (
	cfgLok          sync.Mutex
	config          Configs
	pwd             string
	backConnStepAll = make(chan int, 1)
)

// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.ToLower(s)
	return s
}

func cleanIp(s string) string {
	s = strings.Replace(s, ":", "", -1)  // Remove spaces
	s = strings.Replace(s, ".", "_", -1) // Remove open parenthesis
	return s
}

func loadConfig() bool {
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	pwd = path
	content, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("error: %v", err)
		return false
	} else {
		var c Configs
		err := yaml.Unmarshal(content, &c)
		if err != nil {
			log.Fatalf("error: %v", err)
			return false
		}
		cfgLok.Lock()
		oldconfig := config
		go CloseConnection(oldconfig)
		config = c
		cfgLok.Unlock()
		return true
	}
}

func WriteLog(message string) {
	fh, err := os.OpenFile(pwd+"/"+*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		fh.Seek(0, 2)
		fh.WriteString(time.Now().Format("2006-01-02 15:04:05") + " " + message + "\n")
	}
	if fh != nil {
		fh.Close()
	}
}
