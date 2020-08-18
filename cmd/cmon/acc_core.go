package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/csv"
)

type (
	statItem struct {
		Period []int
		Value  []float32
	}

	ec2Item struct {
		Acct   string
		Type   string
		Plat   string
		AZ     string
		AMI    string
		Spot   string
		Tag    map[string]string
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
		Tag    map[string]string
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
		Tag     map[string]string
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

func gopher(src string, m *model, update func(*model, map[string]string, int)) {
	start, acc, token, pages, items, meta, now := int(time.Now().Unix()), make(chan accTok, 1), accTok(0), 0, 0, false, 0
	pygo := exec.Command("python", fmt.Sprintf("%v/gopher.py", strings.TrimRight(settings.BinDir, "/")), src)
	defer func() {
		if e, x := recover(), pygo.Wait(); e != nil {
			logE.Printf("gopher error fetching from %q: %v", src, e.(error))
		} else if x != nil {
			logE.Printf("gopher errors fetching from %q: %v", src, x.(*exec.ExitError).Stderr)
		} else {
			logI.Printf("gopher fetched %v items in %v pages from %q", items, pages, src)
			m.req <- modRq{atEXCL, acc}
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
		m.req <- modRq{atEXCL, acc}
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

func flush(n string, m *model, at accTyp, release bool) {
	acc := make(chan accTok, 1)
	m.req <- modRq{at, acc}
	token := <-acc

	b, err := json.MarshalIndent(m.data[0], "", "\t")
	if release {
		m.rel <- token
	}
	if err != nil {
		logE.Printf("can't encode %q state to JSON: %v", n, err)
	} else if err = ioutil.WriteFile(settings.Models[n], b, 0644); err != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], err)
	}
}

func ec2awsBoot(n string, ctl chan string) {
	ec2, f, m := &ec2Model{Inst: make(map[string]*ec2Item, 512)}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, ec2); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	m.data = append(m.data, ec2)
	ctl <- n
}
func ec2awsGopher(m *model, item map[string]string, now int) {
	ec2 := m.data[0].(*ec2Model)
	if item == nil {
		if now > ec2.Current {
			ec2.Current = now
		}
		return
	}
	inst := ec2.Inst[item["id"]]
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
	if tag := item["tag"]; tag != "" {
		inst.Tag = make(map[string]string)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			inst.Tag[kvs[0]] = kvs[1]
		}
	} else {
		inst.Tag = nil
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
func ec2awsGarbage(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// collect the garbage
	m.rel <- token
}
func ec2awsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, ec2awsGopher) })
	goAfter(240*time.Second, 270*time.Second, func() { ec2awsGarbage(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, gc, fl :=
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, ec2awsGopher) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher("stats."+n, m, statsec2awsGopher)})
		case <-gc.C:
			goAfter(240*time.Second, 270*time.Second, func() { ec2awsGarbage(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	flush(n, mMod[n], atEXCL, false)
	ctl <- n
}

func ebsawsBoot(n string, ctl chan string) {
	ebs, f, m := &ebsModel{Vol: make(map[string]*ebsItem, 1024)}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, ebs); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	m.data = append(m.data, ebs)
	ctl <- n
}
func ebsawsGopher(m *model, item map[string]string, now int) {
	ebs := m.data[0].(*ebsModel)
	if item == nil {
		if now > ebs.Current {
			ebs.Current = now
		}
		return
	}
	vol := ebs.Vol[item["id"]]
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
	if tag := item["tag"]; tag != "" {
		vol.Tag = make(map[string]string)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			vol.Tag[kvs[0]] = kvs[1]
		}
	} else {
		vol.Tag = nil
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
func ebsawsGarbage(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// collect the garbage
	m.rel <- token
}
func ebsawsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, ebsawsGopher) })
	goAfter(240*time.Second, 270*time.Second, func() { ebsawsGarbage(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, gc, fl :=
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, ebsawsGopher) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher("stats."+n, m, statsebsawsGopher)})
		case <-gc.C:
			goAfter(240*time.Second, 270*time.Second, func() { ebsawsGarbage(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func ebsawsTerm(n string, ctl chan string) {
	flush(n, mMod[n], atEXCL, false)
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	rds, f, m := &rdsModel{DB: make(map[string]*rdsItem, 128)}, settings.Models[n], mMod[n]
	if b, err := ioutil.ReadFile(f); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, f)
	} else if err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, f, err)
	} else if err = json.Unmarshal(b, rds); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, f, err)
	}
	m.data = append(m.data, rds)
	ctl <- n
}
func rdsawsGopher(m *model, item map[string]string, now int) {
	rds := m.data[0].(*rdsModel)
	if item == nil {
		if now > rds.Current {
			rds.Current = now
		}
		return
	}
	db := rds.DB[item["id"]]
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
	if tag := item["tag"]; tag != "" {
		db.Tag = make(map[string]string)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			db.Tag[kvs[0]] = kvs[1]
		}
	} else {
		db.Tag = nil
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
func rdsawsGarbage(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// collect the garbage
	m.rel <- token
}
func rdsawsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, rdsawsGopher) })
	goAfter(240*time.Second, 270*time.Second, func() { rdsawsGarbage(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, gc, fl :=
		time.NewTicker(720*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, rdsawsGopher) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher("stats."+n, m, statsrdsawsGopher)})
		case <-gc.C:
			goAfter(240*time.Second, 270*time.Second, func() { rdsawsGarbage(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	flush(n, mMod[n], atEXCL, false)
	ctl <- n
}

func atoi(s string, d int) int {
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	return d
}
func goAfter(a time.Duration, b time.Duration, f func()) {
	time.AfterFunc(a+time.Duration(rand.Int63n(int64(b-a))), f)
}
