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
		Acct   string
		Type   string
		Plat   string
		AZ     string
		AMI    string
		Spot   string
		Tags   map[string]string
		State  string
		Since  int
		Last   int
		Active []int
		Stats  map[string]statItem
	}
	ec2Model struct {
		Current int
		Inst    map[string]*ec2Item
	}

	ebsItem struct {
		Acct   string
		Type   string
		Size   int
		IOPS   int
		AZ     string
		Mount  string
		Tags   map[string]string
		State  string
		Since  int
		Last   int
		Active []int
		Stats  map[string]statItem
	}
	ebsModel struct {
		Current int
		Vol     map[string]*ebsItem
	}

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
		Last    int
		Active  []int
		Stats   map[string]statItem
	}
	rdsModel struct {
		Current int
		DB      map[string]*rdsItem
	}
)

func gopher(src string, m *model, at accTyp, update func(*model, map[string]string, int)) {
	start, acc, token, pages, items, meta, now := int(time.Now().Unix()), make(chan accTok, 1), accTok(0), 0, 0, false, 0
	pygo := exec.Command("python", fmt.Sprintf("%v/gopher.py", strings.TrimRight(settings.BinDir, "/")), src)
	defer func() {
		if e, x := recover(), pygo.Wait(); e != nil {
			logE.Printf("gopher error fetching from %q: %v", src, e.(error))
		} else if x != nil {
			logE.Printf("gopher errors fetching from %q: %v", src, x.(*exec.ExitError).Stderr)
		} else {
			logI.Printf("gopher fetched %v items in %v pages from %q", items, pages, src)
			m.req <- modRq{at, acc}
			token = <-acc
			update(m, nil, start) // TODO: should this be called even on errors?
			m.rel <- token
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
	for item := range in {
		now = int(time.Now().Unix())
		pages++
		m.req <- modRq{at, acc}
		token = <-acc
		for {
			if _, meta = item["~meta"]; !meta {
				update(m, item, now)
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
	ec2, f, m := &ec2Model{}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, ec2); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	if ec2.Inst == nil {
		ec2.Inst = make(map[string]*ec2Item)
	}
	m.data = append(m.data, ec2)
	ctl <- n
}
func ec2awsGopher(m *model, item map[string]string, now int) {
	// directly insert item into pre-aquired model
	ec2 := m.data[0].(*ec2Model)
	if item == nil {
		ec2.Current = now
		return
	}
	inst, _ := ec2.Inst[item["id"]]
	if inst == nil {
		inst = &ec2Item{
			Type:  item["type"],
			Plat:  item["plat"],
			AMI:   item["ami"],
			Spot:  item["spot"],
			Since: now,
		}
		ec2.Inst[item["id"]] = inst
	}
	inst.Acct = item["acct"]
	inst.AZ = item["az"]
	if tags := item["tags"]; tags != "" {
		inst.Tags = make(map[string]string)
		for _, kv := range strings.Split(tags, "\t") {
			kvs := strings.Split(kv, "=")
			inst.Tags[kvs[0]] = kvs[1]
		}
	} else {
		inst.Tags = nil
	}
	if inst.State = item["state"]; inst.State == "running" {
		if inst.Active == nil || inst.Last > inst.Active[len(inst.Active)-1] {
			inst.Active = append(inst.Active, now, now)
		} else {
			inst.Active[len(inst.Active)-1] = now
		}
	}
	inst.Last = now
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
			//go gopher("stats."+n, m, atEXCL, statsec2awsGopher)
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
	ebs, f, m := &ebsModel{}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, ebs); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	if ebs.Vol == nil {
		ebs.Vol = make(map[string]*ebsItem)
	}
	m.data = append(m.data, ebs)
	ctl <- n
}
func ebsawsGopher(m *model, item map[string]string, now int) {
	// directly insert item into pre-aquired model
	ebs := m.data[0].(*ebsModel)
	if item == nil {
		ebs.Current = now
		return
	}
	vol, _ := ebs.Vol[item["id"]]
	if vol == nil {
		vol = &ebsItem{
			Type:  item["type"],
			Since: now,
		}
		ebs.Vol[item["id"]] = vol
	}
	vol.Acct = item["acct"]
	vol.Size = atoi(item["size"], -1)
	vol.IOPS = atoi(item["iops"], -1)
	vol.AZ = item["az"]
	vol.Mount = item["mount"]
	if tags := item["tags"]; tags != "" {
		vol.Tags = make(map[string]string)
		for _, kv := range strings.Split(tags, "\t") {
			kvs := strings.Split(kv, "=")
			vol.Tags[kvs[0]] = kvs[1]
		}
	} else {
		vol.Tags = nil
	}
	if vol.State = item["state"]; vol.State == "in-use" {
		if vol.Active == nil || vol.Last > vol.Active[len(vol.Active)-1] {
			vol.Active = append(vol.Active, now, now)
		} else {
			vol.Active[len(vol.Active)-1] = now
		}
	}
	vol.Last = now
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
			//go gopher("stats."+n, m, atEXCL, statsebsawsGopher)
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
	rds, f, m := &rdsModel{}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, rds); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	if rds.DB == nil {
		rds.DB = make(map[string]*rdsItem)
	}
	m.data = append(m.data, rds)
	ctl <- n
}
func rdsawsGopher(m *model, item map[string]string, now int) {
	// directly insert item into pre-aquired model
	rds := m.data[0].(*rdsModel)
	if item == nil {
		rds.Current = now
		return
	}
	db, _ := rds.DB[item["id"]]
	if db == nil {
		db = &rdsItem{
			Type:   item["type"],
			SType:  item["stype"],
			Engine: item["engine"],
			Since:  now,
		}
		rds.DB[item["id"]] = db
	}
	db.Acct = item["acct"]
	db.Size = atoi(item["size"], -1)
	db.Ver = item["ver"]
	db.AZ = item["az"]
	db.Lic = item["lic"]
	db.MultiAZ = item["multiaz"] == "True"
	if tags := item["tags"]; tags != "" {
		db.Tags = make(map[string]string)
		for _, kv := range strings.Split(tags, "\t") {
			kvs := strings.Split(kv, "=")
			db.Tags[kvs[0]] = kvs[1]
		}
	} else {
		db.Tags = nil
	}
	if db.State = item["state"]; db.State == "available" {
		if db.Active == nil || db.Last > db.Active[len(db.Active)-1] {
			db.Active = append(db.Active, now, now)
		} else {
			db.Active[len(db.Active)-1] = now
		}
	}
	db.Last = now
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
			//go gopher("stats."+n, m, atEXCL, statsrdsawsGopher)
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
