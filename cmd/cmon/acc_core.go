package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
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
		Since   int
		Updated int
		Active  []int
		Stats   map[string]statItem
	}
	ec2Model map[string]*ec2Item

	ebsItem struct {
		Acct    string
		Type    string
		Size    int
		IOPS    int
		AZ      string
		Mount   string
		Tags    map[string]string
		State   string
		Since   int
		Updated int
		Active  []int
		Stats   map[string]statItem
	}
	ebsModel map[string]*ebsItem

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
		Since   int
		Updated int
		Active  []int
		Stats   map[string]statItem
	}
	rdsModel map[string]*rdsItem
)

func gopher(src string, m *model, at accTyp, update func(*model, map[string]string, string, int)) {
	pygo, items := exec.Command("python", fmt.Sprintf("%v/gopher.py", strings.TrimRight(settings.BinDir, "/")), src), 0
	defer func() {
		if e, x := recover(), pygo.Wait(); e != nil {
			// TODO: consider if access token can be released here
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
	acc, meta, now, token := make(chan accTok, 1), false, 0, accTok(0)
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
	m, f, data := make(ec2Model), settings.Models[n], mMod[n].data
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, &m); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	data = append(data, m)
	ctl <- n
}
func ec2awsGopher(m *model, item map[string]string, src string, now int) {
	// directly insert item into pre-aquired model
	ec2, id := m.data[0].(ec2Model), item["id"]
	i, _ := ec2[id]
	if i == nil {
		i = &ec2Item{
			Type:  item["type"],
			Plat:  item["plat"],
			AMI:   item["ami"],
			Spot:  item["spot"],
			Since: now,
		}
		ec2[id] = i
	}
	i.Acct = item["acct"]
	i.AZ = item["az"]
	if i.State = item["state"]; i.State == "running" {
		if i.Active == nil || i.Updated > i.Active[len(i.Active)-1] {
			i.Active = append(i.Active, now, now)
		} else {
			i.Active[len(i.Active)-1] = now
		}
	}
	i.Updated = now
}
func ec2awsMaintS(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func ec2awsMaintX(m *model) {
	acc := make(chan accTok, 1)
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
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	<-acc

	// persist object model state for shutdown; term accessors don't release object
	if b, err := json.MarshalIndent(m.data[0], "", "\t"); err != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, err)
	} else if err = ioutil.WriteFile(settings.Models[n], b, 0644); err != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], err)
	}
	ctl <- n
}

func ebsawsBoot(n string, ctl chan string) {
	m, f, data := make(ebsModel), settings.Models[n], mMod[n].data
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, &m); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	data = append(data, m)
	ctl <- n
}
func ebsawsGopher(m *model, item map[string]string, src string, now int) {
	// directly insert item into pre-aquired model
	ebs, id := m.data[0].(ebsModel), item["id"]
	v, _ := ebs[id]
	if v == nil {
		v = &ebsItem{
			Type:  item["type"],
			Since: now,
		}
		ebs[id] = v
	}
	v.Acct = item["acct"]
	v.Size = atoi(item["size"], -1)
	v.IOPS = atoi(item["iops"], -1)
	v.AZ = item["az"]
	v.Mount = item["mount"]
	if v.State = item["state"]; v.State == "in-use" {
		if v.Active == nil || v.Updated > v.Active[len(v.Active)-1] {
			v.Active = append(v.Active, now, now)
		} else {
			v.Active[len(v.Active)-1] = now
		}
	}
	v.Updated = now
}
func ebsawsMaintS(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func ebsawsMaintX(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	m.rel <- token
}
func ebsawsMaint(n string) {
	for m, st, xt, gt, gtalt := mMod[n], time.NewTicker(6*time.Second), time.NewTicker(90*time.Second),
		time.NewTicker(600*time.Second), time.NewTicker(3600*time.Second); ; {
		select {
		case <-st.C:
			go ebsawsMaintS(m)
		case <-xt.C:
			go ebsawsMaintX(m)
		case <-gt.C:
			go gopher(n, m, atEXCL, ebsawsGopher)
		case <-gtalt.C:
			//go gopher(n+"/stats", m, atEXCL, ebsawsGopher)
		}
	}
}
func ebsawsTerm(n string, ctl chan string) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	<-acc

	// persist object model state for shutdown; term accessors don't release object
	if b, err := json.MarshalIndent(m.data[0], "", "\t"); err != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, err)
	} else if err = ioutil.WriteFile(settings.Models[n], b, 0644); err != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], err)
	}
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	m, f, data := make(rdsModel), settings.Models[n], mMod[n].data
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, &m); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	data = append(data, m)
	ctl <- n
}
func rdsawsGopher(m *model, item map[string]string, src string, now int) {
	// directly insert item into pre-aquired model
	rds, id := m.data[0].(rdsModel), item["id"]
	db, _ := rds[id]
	if db == nil {
		db = &rdsItem{
			Type:   item["type"],
			SType:  item["stype"],
			Engine: item["engine"],
			Since:  now,
		}
		rds[id] = db
	}
	db.Acct = item["acct"]
	db.Size = atoi(item["size"], -1)
	db.Ver = item["ver"]
	db.AZ = item["az"]
	db.Lic = item["lic"]
	db.MultiAZ = item["multiaz"] == "True"
	if db.State = item["state"]; db.State == "available" {
		if db.Active == nil || db.Updated > db.Active[len(db.Active)-1] {
			db.Active = append(db.Active, now, now)
		} else {
			db.Active[len(db.Active)-1] = now
		}
	}
	db.Updated = now
}
func rdsawsMaintS(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func rdsawsMaintX(m *model) {
	acc := make(chan accTok, 1)
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
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	<-acc

	// persist object model state for shutdown; term accessors don't release object
	if b, e := json.MarshalIndent(m.data[0], "", "\t"); e != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, e)
	} else if e = ioutil.WriteFile(settings.Models[n], b, 0644); e != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], e)
	}
	ctl <- n
}

func atoi(s string, d int) int {
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	return d
}
