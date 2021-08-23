package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var testconnwg sync.WaitGroup

func (e *Exporter) execConn(testStepAll chan int) {
	select {
	case testStepAll <- 1:
	default:
		return
	}
	defer func() {
		<-testStepAll
	}()

	pg, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(pg, "-testconn")
	cc := strings.Builder{}
	cmd.Stderr = &cc
	cmd.Run()
	strs := strings.Split(cc.String(), "\n")
	for _, v := range strs {
		if strings.Contains(v, "query time") {
			fs := strings.Split(v, " ")
			if len(fs) == 4 {
				connstr := fs[2]
				ipport, svname := splitConnStr(connstr)
				ts := fs[3]
				if strings.HasSuffix(ts, "ms") {
					ts = strings.Replace(ts, "ms", "", 1)
					dr, err := strconv.ParseFloat(ts, 64)
					if err != nil {
						e.used_times.WithLabelValues(ipport, svname).Set(999)
						continue
					}
					e.used_times.WithLabelValues(ipport, svname, "connectsucc").Set(dr / 1000)
				} else {
					ts = strings.Replace(ts, "s", "", 1)
					dr, err := strconv.ParseFloat(ts, 64)
					if err != nil {
						e.used_times.WithLabelValues(ipport, svname).Set(999)
						continue
					}
					e.used_times.WithLabelValues(ipport, svname, "connectsucc").Set(dr)
				}
			}
		}
	}
}

func testConnects() {

	for _, v := range config.Cfgs {
		testconnwg.Add(1)
		go testConn(v.Connection)
	}

	testconnwg.Wait()
}

func testConn(str string) {
	defer testconnwg.Done()
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
	defer cancel()

	db, err := sql.Open("oracle", str)
	if err != nil {
		log.Infoln(" open ", str, "  err ", err)
		return
	}
	defer func() {
		err = db.Close()
	}()

	err = db.PingContext(ctx)
	if err != nil {
		log.Infoln(" ping ", str, "  err ", err)
		return
	}

	log.Infoln(" ping time  ", str, time.Since(t0))

	var dbname, inname, hostname string
	err = db.QueryRowContext(ctx, "select name, instance_name, host_name from v$database, v$instance").Scan(&dbname, &inname, &hostname)
	if err != nil {
		log.Infoln(" select err ", err)
		return
	}
	os.Stderr.WriteString(fmt.Sprintf("query time %s %v\n", str, time.Since(t0)))

}
