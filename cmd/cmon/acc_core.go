package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/sententico/cost/csv"
)

type (
	ec2Inst struct {
		Typ, OS string
	}
	modEC2 map[string]*ec2Inst
)

func gopher(n string, m *model, at accTyp, update func(*model, map[string]string)) {
	gpath := fmt.Sprintf("%v/gopher.py", strings.TrimRight(settings.BinDir, "/"))
	pygo, rows := exec.Command("python", gpath, n), 0
	defer func() {
		if e, x := recover(), pygo.Wait(); e != nil {
			logE.Printf("gopher error fetching %v: %v", n, e.(error))
		} else if x != nil {
			logE.Printf("gopher errors: %v", x.(*exec.ExitError).Stderr)
		} else {
			logI.Printf("gopher fetched %v rows from %v", rows, n)
		}
	}()
	sb, e := json.MarshalIndent(settings, "", "\t")
	if e != nil {
		panic(e)
	}
	pygo.Stdin = bytes.NewBuffer(sb)
	pipe, e := pygo.StdoutPipe()
	if e != nil {
		panic(e)
	}
	if e = pygo.Start(); e != nil {
		panic(e)
	}

	res := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	if e = res.Open(pipe); e != nil {
		panic(e)
	}
	in, err := res.Get()
	acc, meta, token := make(chan uint32, 1), false, uint32(0)
	for row := range in {
		m.req <- modRq{at, acc}
		token = <-acc
		for {
			if _, meta = row["~meta"]; !meta {
				update(m, row)
				rows++
			}
			select {
			case row = <-in:
				if row != nil {
					continue
				}
			default:
			}
			m.rel <- token
			break
		}
	}
	res.Close()
	if e = <-err; e != nil {
		panic(e)
	}
}

func ec2awsBoot(n string, ctl chan string) {
	m := mMod[n]
	// read/build object model
	m.data = modEC2{
		"i-dog": {"m5.2xlarge", "linux"},
		"i-cat": {"m5.large", "DOS"},
	}
	ctl <- n
}
func ec2awsGopher(m *model, row map[string]string) {
	// directly insert row data into pre-aquired model
}
func ec2awsMaintS(m *model) {
	acc := make(chan uint32, 1)
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func ec2awsMaintX(m *model) {
	acc := make(chan uint32, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	m.rel <- token
}
func ec2awsMaint(n string) {
	for m, st, xt, gt := mMod[n], time.NewTicker(6*time.Second), time.NewTicker(90*time.Second),
		time.NewTicker(600*time.Second); ; {
		select {
		case <-st.C:
			go ec2awsMaintS(m)
		case <-xt.C:
			go ec2awsMaintX(m)
		case <-gt.C:
			go gopher(n, m, atEXCL, ec2awsGopher)
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	m := mMod[n]
	mr := modRq{atEXCL, make(chan uint32, 1)}
	m.req <- mr
	<-mr.acc
	// persist object model for shutdown; term accessors don't release object
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	m := mMod[n]
	// read/build object model
	m.data = nil
	ctl <- n
}
func rdsawsGopher(m *model, row map[string]string) {
	// directly insert row data into pre-aquired model
}
func rdsawsMaintS(m *model) {
	acc := make(chan uint32, 1)
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func rdsawsMaintX(m *model) {
	acc := make(chan uint32, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	m.rel <- token
}
func rdsawsMaint(n string) {
	for m, st, xt, gt := mMod[n], time.NewTicker(6*time.Second), time.NewTicker(90*time.Second),
		time.NewTicker(1200*time.Second); ; {
		select {
		case <-st.C:
			go rdsawsMaintS(m)
		case <-xt.C:
			go rdsawsMaintX(m)
		case <-gt.C:
			go gopher(n, m, atEXCL, rdsawsGopher)
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	m := mMod[n]
	mr := modRq{atEXCL, make(chan uint32, 1)}
	m.req <- mr
	<-mr.acc
	// persist object model for shutdown; term accessors don't release object
	ctl <- n
}
