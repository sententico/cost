package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"

	"github.com/sententico/cost/csv"
)

type (
	statItem struct {
		Periods []int
		Values  []float32
	}

	ec2Item struct {
		Acct    string
		Type    string
		Plat    string
		AZ      string
		AMI     string
		Spot    string
		Tags    map[string]string
		State   string
		Updated int
		Active  []int
		Stats   map[string]*statItem
	}
	ec2Model map[string]*ec2Item

	rdsItem struct {
		Acct    string
		Type    string
		SType   string
		Size    int
		Engine  string
		Ver     string
		Lic     string
		AZ      string
		MultiAZ bool
		Tags    map[string]string
		State   string
		Created int
		Updated int
		Active  []int
		Stats   map[string]*statItem
	}
	rdsModel map[string]*rdsItem
)

func gopher(src string, m *model, at accTyp, update func(*model, map[string]string, string, int)) {
	pygo, items := exec.Command("python", fmt.Sprintf("%v/gopher.py", strings.TrimRight(settings.BinDir, "/")), src), 0
	defer func() {
		if e, x := recover(), pygo.Wait(); e != nil {
			logE.Printf("gopher error fetching from %q: %v", src, e.(error))
		} else if x != nil {
			logE.Printf("gopher errors fetching from %q: %v", src, x.(*exec.ExitError).Stderr)
		} else {
			logI.Printf("gopher fetched %v items from %q", items, src)
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
	} else if e = pygo.Start(); e != nil {
		panic(e)
	}

	res := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	if e = res.Open(pipe); e != nil {
		panic(e)
	}
	in, err := res.Get()
	acc, meta, now, token := make(chan uint32, 1), false, 0, uint32(0)
	for item := range in {
		now = int(time.Now().Unix())
		m.req <- modRq{at, acc}
		token = <-acc
		for {
			if _, meta = item["~meta"]; !meta {
				update(m, item, src, now)
				items++
			}
			select {
			case item = <-in:
				if item != nil {
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
	m, f := make(ec2Model), settings.Models[n]
	if b, err := ioutil.ReadFile(f); err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, &m); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	mMod[n].data = m
	ctl <- n
}
func ec2awsGopher(m *model, item map[string]string, src string, now int) {
	// directly insert item into pre-aquired model
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
	for m, st, xt, gt, gtalt := mMod[n], time.NewTicker(6*time.Second), time.NewTicker(90*time.Second),
		time.NewTicker(600*time.Second), time.NewTicker(3600*time.Second); ; {
		select {
		case <-st.C:
			go ec2awsMaintS(m)
		case <-xt.C:
			go ec2awsMaintX(m)
		case <-gt.C:
			go gopher(n, m, atEXCL, ec2awsGopher)
		case <-gtalt.C:
			//go gopher(n+"/stats", m, atEXCL, ec2awsGopher)
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	m, acc := mMod[n], make(chan uint32, 1)
	m.req <- modRq{atEXCL, acc}
	<-acc

	// persist object model state for shutdown; term accessors don't release object
	if b, e := json.MarshalIndent(m.data, "", "\t"); e != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, e)
	} else if e = ioutil.WriteFile(settings.Models[n], b, 0644); e != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], e)
	}
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	m, f := make(rdsModel), settings.Models[n]
	if b, err := ioutil.ReadFile(f); err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, &m); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	mMod[n].data = m
	ctl <- n
}
func rdsawsGopher(m *model, item map[string]string, src string, now int) {
	// directly insert item into pre-aquired model
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
	for m, st, xt, gt, gtalt := mMod[n], time.NewTicker(6*time.Second), time.NewTicker(90*time.Second),
		time.NewTicker(1200*time.Second), time.NewTicker(3600*time.Second); ; {
		select {
		case <-st.C:
			go rdsawsMaintS(m)
		case <-xt.C:
			go rdsawsMaintX(m)
		case <-gt.C:
			go gopher(n, m, atEXCL, rdsawsGopher)
		case <-gtalt.C:
			//go gopher(n+"/stats", m, atEXCL, rdsawsGopher)
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	m, acc := mMod[n], make(chan uint32, 1)
	m.req <- modRq{atEXCL, acc}
	<-acc

	// persist object model state for shutdown; term accessors don't release object
	if b, e := json.MarshalIndent(m.data, "", "\t"); e != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, e)
	} else if e = ioutil.WriteFile(settings.Models[n], b, 0644); e != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], e)
	}
	ctl <- n
}
