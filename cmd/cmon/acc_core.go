package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
		Plat   string            `json:",omitempty"`
		AZ     string            `json:",omitempty"`
		AMI    string            `json:",omitempty"`
		Spot   string            `json:",omitempty"`
		Tag    map[string]string `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int               `json:",omitempty"`
		Stats  map[string]statItem `json:",omitempty"`
	}
	ec2Model struct {
		Current int
		Inst    map[string]*ec2Item
	}

	ebsItem struct {
		Acct   string
		Type   string
		Size   int               `json:",omitempty"`
		IOPS   int               `json:",omitempty"`
		AZ     string            `json:",omitempty"`
		Mount  string            `json:",omitempty"`
		Tag    map[string]string `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int               `json:",omitempty"`
		Stats  map[string]statItem `json:",omitempty"`
	}
	ebsModel struct {
		Current int
		Vol     map[string]*ebsItem
	}

	rdsItem struct {
		Acct    string
		Type    string
		SType   string            `json:",omitempty"`
		Size    int               `json:",omitempty"`
		Engine  string            `json:",omitempty"`
		Ver     string            `json:",omitempty"`
		Lic     string            `json:",omitempty"`
		AZ      string            `json:",omitempty"`
		MultiAZ bool              `json:",omitempty"`
		Tag     map[string]string `json:",omitempty"`
		State   string
		Since   int
		Last    int
		Active  []int               `json:",omitempty"`
		Stats   map[string]statItem `json:",omitempty"`
	}
	rdsModel struct {
		Current int
		DB      map[string]*rdsItem
	}

	cdrStat struct {
		Cost  float64 // total USD
		Dur   uint64  // total 0.1s actual (high-order 16 bits unused)
		Calls uint32  // total count
	}
	termSum struct {
		Current int32                        // hour cursor in summary maps (Unix time)
		ByHour  map[int32]cdrStat            // map by hour (Unix time)
		ByGeo   map[int32]map[string]cdrStat // map by hour/geo-code
		ByFrom  map[int32]map[uint64]cdrStat // map by hour/E.164 number
		ByTo    map[int32]map[uint64]cdrStat // map by hour/E.164 prefix (minimally CC, 0x3ff mask)
	}
	cdrItem struct {
		From  uint64  // CC (0x3ff); area digit len (0x3c00); E.164 (high-order 50 bits)
		To    uint64  // CC (0x3ff); area digit len (0x3c00); E.164 (high-order 50 bits)
		Begin int32   // Unix time (seconds past epoch GMT)
		Dur   uint32  // 0.1s actual (0x03ffffff mask); applicable rounding (0xfc000000 opt)
		Cost  float32 // USD
	}
	termDetail struct {
		Current int32                         // hour cursor in CDR map (Unix time)
		CDR     map[int32]map[uint64]*cdrItem // map by hour(Unix time) / CDR ID
	}
)

var (
	unleash = getUnleash()
)

func getUnleash() func(string, ...string) *exec.Cmd {
	sfx := map[string]string{
		"aws":  "goph_aws.py",
		"az":   "goph_az.py",
		"gcs":  "goph_gcs.py",
		"k8s":  "goph_k8s.py",
		"rack": "goph_rack.py",
		"cdr":  "goph_cdr.py",
		"":     "goph_aws.py", // must have default (empty suffix)
	}
	return func(src string, options ...string) *exec.Cmd {
		for i := 0; ; i++ {
			if sfx[src[i:]] != "" {
				args := []string{
					"python",
					fmt.Sprintf("%v/%v", strings.TrimRight(settings.BinDir, "/"), sfx[src[i:]]),
				}
				return exec.Command(args[0], append(append(args[1:], options...), src)...)
			}
		}
	}
}

func gopher(src string, m *model, update func(*model, map[string]string, int)) {
	start, acc, token, pages, items, meta, now := int(time.Now().Unix()), make(chan accTok, 1), accTok(0), 0, 0, false, 0
	goph := unleash(src)
	defer func() {
		if e, x := recover(), goph.Wait(); e != nil {
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
		panic(e) // TODO: test that panics here before Start() aren't a problem for deferred Wait()
	}
	goph.Stdin = bytes.NewBuffer(sb)
	pipe, e := goph.StdoutPipe()
	if e != nil {
		panic(e)
	} else if e = goph.Start(); e != nil {
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
		for token = <-acc; ; {
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

func sync(n string, m *model) {
	if fn := settings.Models[n]; fn == "" {
		logE.Fatalf("no resource configured into which %q state may persist", n)
	} else if f, err := os.Open(fn); os.IsNotExist(err) {
		logW.Printf("no %q state found at %q", n, fn)
	} else if dec, pdata := json.NewDecoder(f), m.data[0:m.persist]; err != nil {
		logE.Fatalf("cannot read %q state from %q: %v", n, fn, err)
	} else if err = dec.Decode(&pdata); err != nil {
		logE.Fatalf("%q state resource %q is invalid JSON: %v", n, fn, err)
	} else {
		f.Close()
	}
}

func flush(n string, m *model, at accTyp, release bool) {
	acc := make(chan accTok, 1)
	m.req <- modRq{at, acc}
	token := <-acc

	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		enc.SetIndent("", "\t")
		if err := enc.Encode(m.data[0:m.persist]); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	if f, err := os.OpenFile(settings.Models[n], os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664); err != nil {
		logE.Printf("can't persist %q state to %q: %v", n, settings.Models[n], err)
		pr.CloseWithError(err)
	} else if _, err = io.Copy(f, pr); err != nil {
		logE.Printf("can't complete persisting %q state to %q: %v", n, settings.Models[n], err)
		pr.CloseWithError(err)
		f.Close()
	} else {
		f.Close()
	}
	if release {
		m.rel <- token
	}
}

func ec2awsBoot(n string, ctl chan string) {
	ec2, m := &ec2Model{Inst: make(map[string]*ec2Item, 512)}, mMod[n]
	m.data = append(m.data, ec2)
	m.persist = len(m.data)
	sync(n, m)
	ctl <- n
}
func ec2awsInsert(m *model, item map[string]string, now int) {
	ec2, id := m.data[0].(*ec2Model), item["id"]
	if item == nil {
		if now > ec2.Current {
			ec2.Current = now
		}
		return
	} else if id == "" {
		return
	}
	inst := ec2.Inst[id]
	if inst == nil {
		inst = &ec2Item{
			Type:  item["type"],
			Plat:  item["plat"],
			AMI:   item["ami"],
			Spot:  item["spot"],
			Since: now,
		}
		ec2.Inst[id] = inst
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
func ec2awsClean(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// clean expired data (including case of id=="")
	m.rel <- token
}
func ec2awsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, ec2awsInsert) })
	goAfter(240*time.Second, 270*time.Second, func() { ec2awsClean(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, cl, fl :=
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, ec2awsInsert) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher(n+"/stats", m, ec2awsSInsert)})
		case <-cl.C:
			goAfter(240*time.Second, 270*time.Second, func() { ec2awsClean(m) })
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
	ebs, m := &ebsModel{Vol: make(map[string]*ebsItem, 1024)}, mMod[n]
	m.data = append(m.data, ebs)
	m.persist = len(m.data)
	sync(n, m)
	ctl <- n
}
func ebsawsInsert(m *model, item map[string]string, now int) {
	ebs, id := m.data[0].(*ebsModel), item["id"]
	if item == nil {
		if now > ebs.Current {
			ebs.Current = now
		}
		return
	} else if id == "" {
		return
	}
	vol := ebs.Vol[id]
	if vol == nil {
		vol = &ebsItem{
			Type:  item["type"],
			Since: now,
		}
		ebs.Vol[id] = vol
	}
	vol.Acct = item["acct"]
	vol.Size = atoi(item["size"], -1)
	vol.IOPS = atoi(item["iops"], -1)
	vol.AZ = item["az"]
	if vol.Mount = item["mount"]; vol.Mount == "0 attachments" {
		vol.Mount = ""
	}
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
func ebsawsClean(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// clean expired data (including case of id=="")
	m.rel <- token
}
func ebsawsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, ebsawsInsert) })
	goAfter(240*time.Second, 270*time.Second, func() { ebsawsClean(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, cl, fl :=
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, ebsawsInsert) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher(n+"/stats", m, ebsawsSInsert)})
		case <-cl.C:
			goAfter(240*time.Second, 270*time.Second, func() { ebsawsClean(m) })
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
	rds, m := &rdsModel{DB: make(map[string]*rdsItem, 128)}, mMod[n]
	m.data = append(m.data, rds)
	m.persist = len(m.data)
	sync(n, m)
	ctl <- n
}
func rdsawsInsert(m *model, item map[string]string, now int) {
	rds, id := m.data[0].(*rdsModel), item["id"]
	if item == nil {
		if now > rds.Current {
			rds.Current = now
		}
		return
	} else if id == "" {
		return
	}
	db := rds.DB[id]
	if db == nil {
		db = &rdsItem{
			Type:   item["type"],
			SType:  item["stype"],
			Engine: item["engine"],
			Since:  now,
		}
		rds.DB[id] = db
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
func rdsawsClean(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// clean expired data (including case of id=="")
	m.rel <- token
}
func rdsawsMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, rdsawsInsert) })
	goAfter(240*time.Second, 270*time.Second, func() { rdsawsClean(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, sg, cl, fl :=
		time.NewTicker(720*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(86400*time.Second), time.NewTicker(1440*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, rdsawsInsert) })
		case <-sg.C:
			//goAfter(0, 60*time.Second, func() {gopher(n+"/stats", m, rdsawsSInsert)})
		case <-cl.C:
			goAfter(240*time.Second, 270*time.Second, func() { rdsawsClean(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	flush(n, mMod[n], atEXCL, false)
	ctl <- n
}

func termcdrBoot(n string, ctl chan string) {
	sum, detail, m := &termSum{
		ByHour: make(map[int32]cdrStat, 2184),
		ByGeo:  make(map[int32]map[string]cdrStat, 2184),
		ByFrom: make(map[int32]map[uint64]cdrStat, 2184),
		ByTo:   make(map[int32]map[uint64]cdrStat, 2184),
	}, &termDetail{
		CDR: make(map[int32]map[uint64]*cdrItem, 2184),
	}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	sync(n, m)

	// TODO: append third segment to m.data for E.164 decoding and telephony rating (not persisted)
	ctl <- n
}
func termcdrInsert(m *model, item map[string]string, now int) {
	sum, detail, hr, id := m.data[0].(*termSum), m.data[1].(*termDetail), int32(now-now%3600), ato64(item["id"], 0)
	if item == nil {
		if hr > sum.Current {
			sum.Current, detail.Current = hr, hr
		}
		return
	} else if id == 0 {
		return
	}
	cdr, cdrhr := &cdrItem{
		From:  0, // convert item["calling"] to E.164 and encode
		To:    0, // convert item["called"] to E.164 and encode
		Begin: 0, // convert item["date"]/item["time"] to Unix time
		Dur:   0, // convert item["dur"] (ms)
		Cost:  0, // lookup rate for call
	}, detail.CDR[hr]
	// finish setting detail cdr struct
	// update sum maps
	if cdrhr == nil {
		cdrhr = make(map[uint64]*cdrItem, 4096)
	}
	cdrhr[id] = cdr
}
func termcdrClean(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// sum, detail := m.data[0].(*termSum), m.data[1].(*termDetail)
	// clean expired data (including case of id==0)
	// sum.ByFrom and detail.CDR maps need aggressive trimming
	m.rel <- token
}
func termcdrMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, termcdrInsert) })
	goAfter(240*time.Second, 270*time.Second, func() { termcdrClean(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, cl, fl :=
		time.NewTicker(360*time.Second),
		time.NewTicker(21600*time.Second), time.NewTicker(2880*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, termcdrInsert) })
		case <-cl.C:
			goAfter(240*time.Second, 270*time.Second, func() { termcdrClean(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func termcdrTerm(n string, ctl chan string) {
	flush(n, mMod[n], atEXCL, false)
	ctl <- n
}

func atoi(s string, d int) int {
	if i, err := strconv.ParseInt(s, 0, 0); err == nil {
		return int(i)
	}
	return d
}

func ato64(s string, d uint64) uint64 {
	if i, err := strconv.ParseUint(s, 0, 0); err == nil {
		return i
	}
	return d
}
