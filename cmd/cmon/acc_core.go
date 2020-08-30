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
	"github.com/sententico/cost/tel"
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
		Dur   uint64  // total 0.1s actual (high-order 24 bits unused)
		Calls uint32  // total count (high-order 4 bit unused)
	}
	hS      map[int32]*cdrStat                    // stats by hour
	hgS     map[int32]map[string]*cdrStat         // stats by hour/geo zone
	hpS     map[int32]map[tel.E164digest]*cdrStat // stats by hour/E.164 prefix digest
	termSum struct {
		Current int32 // hour cursor in term summary maps (Unix time)
		ByHour  hS    // map by hour (Unix time)
		ByGeo   hgS   // map by hour/geo zone
		ByFrom  hpS   // map by hour/full from number
		ByTo    hpS   // map by hour/to prefix (CC/CC+np)
	}
	origSum struct {
		Current int32 // hour cursor in orig summary maps (Unix time)
		ByHour  hS    // map by hour (Unix time)
		ByTo    hpS   // map by hour/full to number
	}
	cdrItem struct {
		From  tel.E164digest // decoded from number
		To    tel.E164digest // decoded to number
		Begin int32          // Unix time (seconds past epoch GMT)
		Dur   uint32         // billable duration increment (s) | actual (0.1s)
		Info  uint32         // other info: service provider code | tries
		Cost  float32        // rated USD cost
	}
	hiC        map[int32]map[uint64]*cdrItem // CDRs by hour/ID
	termDetail struct {
		Current int32 // hour cursor in term CDR map (Unix time)
		CDR     hiC   // map by hour/CDR ID
	}
	origDetail struct {
		Current int32 // hour cursor in orig CDR map (Unix time)
		CDR     hiC   // map by hour/CDR ID
	}
	cdrWork struct {
		decoder tel.Decoder
		trates  tel.Rater
		orates  tel.Rater
		sp      tel.SPmap
		tn      tel.E164full
	}
)

const (
	durMask   = 0x03ff_ffff // CDR Dur actual duration mask (0.1s)
	billShift = 32 - 6      // CDR Dur billable duration increment (s) shift

	tryMask = 0xff   // CDR Info tries mask
	spShift = 32 - 8 // CDR Info service provider shift
)

var (
	unleash = getUnleash()
)

func getUnleash() func(string, ...string) *exec.Cmd {
	sfx := map[string]string{
		"aws": "goph_aws.py",
		"az":  "goph_az.py",
		"gcs": "goph_gcs.py",
		"k8s": "goph_k8s.py",
		"rax": "goph_rax.py",
		"asp": "goph_asp.py",
		"":    "goph_aws.py", // must have default (empty suffix)
	}
	return func(src string, options ...string) *exec.Cmd {
		for i := 0; ; i++ {
			if sfx[src[i:]] != "" {
				args := []string{
					"python",
					fmt.Sprintf("%v/%v", strings.TrimRight(settings.BinDir, "/"), sfx[src[i:]]),
				}
				// TODO: change to exec.CommandContext() to support timeouts?
				return exec.Command(args[0], append(append(args[1:], options...), src)...)
			}
		}
	}
}

func gopher(src string, m *model, insert func(*model, map[string]string, int)) {
	goph, eb, start, now := unleash(src), bytes.Buffer{}, int(time.Now().Unix()), 0
	gophStdout := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	acc, token, pages, items, meta := make(chan accTok, 1), accTok(0), 0, 0, false
	defer func() {
		if e := recover(); e != nil {
			if token != 0 {
				m.rel <- token
				gophStdout.Close()
			}
			logE.Printf("gopher error while fetching %q: %v", src, e)
		} else if e := goph.Wait(); e != nil {
			logE.Printf("gopher errors from %q: %v [%v]", src, e, strings.Split(strings.Trim(
				string(eb.Bytes()), "\n\t "), "\n")[0])
		} else if items > 0 {
			logI.Printf("gopher fetched %v items in %v pages from %q", items, pages, src)
		}
		if items > 0 {
			m.req <- modRq{atEXCL, acc}
			token = <-acc
			insert(m, nil, start)
			m.rel <- token
		}
	}()
	if sb, e := json.MarshalIndent(settings, "", "\t"); e != nil {
		panic(e)
	} else if goph.Stdin, goph.Stderr = bytes.NewBuffer(sb), &eb; false {
	} else if pipe, e := goph.StdoutPipe(); e != nil {
		panic(e)
	} else if e = goph.Start(); e != nil {
		panic(e)
	} else if e = gophStdout.Open(pipe); e != nil {
		panic(e)
	}

	results, err := gophStdout.Get()
	for item := range results {
		now = int(time.Now().Unix())
		pages++
		m.req <- modRq{atEXCL, acc}
		for token = <-acc; ; {
			if _, meta = item["~meta"]; !meta {
				insert(m, item, now)
				items++
			}
			select {
			case item = <-results:
				if item != nil {
					continue
				}
			default:
			}
			m.rel <- token
			token = 0
			break
		}
	}
	gophStdout.Close()
	if e := <-err; e != nil {
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

	// clean expired/invalid data
	ec2 := m.data[0].(*ec2Model)
	for id, inst := range ec2.Inst {
		if id == "" || ec2.Current-inst.Last > 86400*365 { // placeholder
			delete(ec2.Inst, id)
		}
	}
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
	vol.Size = atoi(item["size"], 0)
	vol.IOPS = atoi(item["iops"], 0)
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

	// clean expired/invalid data
	ebs := m.data[0].(*ebsModel)
	for id, vol := range ebs.Vol {
		if id == "" || ebs.Current-vol.Last > 86400*365 { // placeholder
			delete(ebs.Vol, id)
		} else {
			if vol.IOPS < 0 {
				vol.IOPS = 0
			}
			if vol.Mount == "0 attachments" {
				vol.Mount = ""
			}
		}
	}
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
	db.Size = atoi(item["size"], 0)
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

	// clean expired/invalid data
	rds := m.data[0].(*rdsModel)
	for id, db := range rds.DB {
		if id == "" || rds.Current-db.Last > 86400*365 { // placeholder
			delete(rds.DB, id)
		}
	}
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

func cdraspBoot(n string, ctl chan string) {
	tsum, osum, tdetail, odetail, work, m := &termSum{
		ByHour: make(hS, 2184),
		ByGeo:  make(hgS, 2184),
		ByFrom: make(hpS, 2184),
		ByTo:   make(hpS, 2184),
	}, &origSum{
		ByHour: make(hS, 2184),
		ByTo:   make(hpS, 2184),
	}, &termDetail{
		CDR: make(hiC, 2184),
	}, &origDetail{
		CDR: make(hiC, 2184),
	}, &cdrWork{}, mMod[n]
	m.data = append(m.data, tsum)
	m.data = append(m.data, osum)
	m.data = append(m.data, tdetail)
	m.data = append(m.data, odetail)
	m.persist = len(m.data)
	sync(n, m)

	work.decoder.NANPbias = true
	work.trates.Default, work.orates.Default = tel.DefaultTermRates, tel.DefaultOrigRates
	if err := work.decoder.Load(nil); err != nil {
		logE.Fatalf("%q cannot load E.164 decoder: %v", n, err)
	} else if err = work.trates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load termination rates: %v", n, err)
	} else if err = work.orates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load origination rates: %v", n, err)
	} else if err = work.sp.Load(nil); err != nil {
		logE.Fatalf("%q cannot load service provider map: %v", n, err)
	}
	m.data = append(m.data, work)
	ctl <- n
}
func (m hiC) add(hr int32, id uint64, cdr *cdrItem) bool {
	if hm := m[hr]; hm == nil {
		hm = make(map[uint64]*cdrItem, 4096)
		m[hr], hm[id] = hm, cdr
	} else if hm[id] == nil {
		hm[id] = cdr
	} else {
		return false
	}
	return true
}
func (m hS) add(hr int32, cdr *cdrItem) {
	if s := m[hr]; s == nil {
		m[hr] = &cdrStat{Calls: 1, Dur: uint64(cdr.Dur & durMask), Cost: float64(cdr.Cost)}
	} else {
		s.Calls++
		s.Dur += uint64(cdr.Dur & durMask)
		s.Cost += float64(cdr.Cost)
	}
}
func (m hgS) add(hr int32, geo string, cdr *cdrItem) {
	if hm := m[hr]; hm == nil {
		hm = make(map[string]*cdrStat)
		m[hr], hm[geo] = hm, &cdrStat{Calls: 1, Dur: uint64(cdr.Dur & durMask), Cost: float64(cdr.Cost)}
	} else if s := hm[geo]; s == nil {
		hm[geo] = &cdrStat{Calls: 1, Dur: uint64(cdr.Dur & durMask), Cost: float64(cdr.Cost)}
	} else {
		s.Calls++
		s.Dur += uint64(cdr.Dur & durMask)
		s.Cost += float64(cdr.Cost)
	}
}
func (m hpS) add(hr int32, pre tel.E164digest, cdr *cdrItem) {
	if hm := m[hr]; hm == nil {
		hm = make(map[tel.E164digest]*cdrStat)
		m[hr], hm[pre] = hm, &cdrStat{Calls: 1, Dur: uint64(cdr.Dur & durMask), Cost: float64(cdr.Cost)}
	} else if s := hm[pre]; s == nil {
		hm[pre] = &cdrStat{Calls: 1, Dur: uint64(cdr.Dur & durMask), Cost: float64(cdr.Cost)}
	} else {
		s.Calls++
		s.Dur += uint64(cdr.Dur & durMask)
		s.Cost += float64(cdr.Cost)
	}
}
func cdraspInsert(m *model, item map[string]string, now int) {
	id, tsum, osum := ato64(item["id"], 0), m.data[0].(*termSum), m.data[1].(*origSum)
	tdetail, odetail, work := m.data[2].(*termDetail), m.data[3].(*origDetail), m.data[4].(*cdrWork)
	b, err := time.Parse(time.RFC3339, item["begin"])
	if err != nil || id == 0 {
		return
	}
	cdr := &cdrItem{
		Begin: int32(b.Unix()),
		Dur:   uint32(atoi(item["dur"], 0)+5) / 10 & durMask,
		Info:  uint32(atoi(item["try"], 1)) & tryMask,
	}

	switch hr := cdr.Begin / 3600; item["type"] {
	case "CARRIER", "SDENUM":
		cdr.To = work.decoder.Digest(item["to"])
		work.decoder.Full(item["from"], &work.tn)
		cdr.From = work.tn.Digest(len(work.tn.Num))
		cdr.Cost = float32(cdr.Dur) / 600 * work.orates.Lookup(&work.tn)
		if tg := item["iTG"]; len(tg) > 6 && tg[:6] == "ASPTIB" {
			cdr.Info |= work.sp.Code(tg[6:]) << spShift
		}
		if hr > osum.Current {
			osum.Current, odetail.Current = hr, hr
		}
		if odetail.CDR.add(hr, id, cdr) {
			osum.ByHour.add(hr, cdr)
			osum.ByTo.add(hr, cdr.To, cdr)
		}
	case "CORE":
	default:
		cdr.From = work.decoder.Digest(item["from"])
		if len(item["dip"]) < 20 || work.decoder.Full(item["dip"][:10], &work.tn) != nil {
			work.decoder.Full(item["to"], &work.tn)
		}
		cdr.To = work.tn.Digest(len(work.tn.Num))
		cdr.Cost = float32(cdr.Dur) / 600 * work.trates.Lookup(&work.tn)
		if tg := item["eTG"]; len(tg) > 6 && tg[:6] == "ASPTOB" {
			cdr.Info |= work.sp.Code(tg[6:]) << spShift
		}
		if hr > tsum.Current {
			tsum.Current, tdetail.Current = hr, hr
		}
		if tdetail.CDR.add(hr, id, cdr) {
			tsum.ByHour.add(hr, cdr)
			tsum.ByGeo.add(hr, work.tn.Geo, cdr)
			tsum.ByFrom.add(hr, cdr.From, cdr)
			tsum.ByTo.add(hr, work.tn.Digest(len(work.tn.CC)+len(work.tn.P)), cdr)
		}
	}
}
func cdraspClean(m *model) {
	acc := make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	tdetail, odetail := m.data[2].(*termDetail), m.data[3].(*origDetail)
	texp, oexp := tdetail.Current-3600*24, odetail.Current-3600*24
	for hr := range tdetail.CDR {
		if hr <= texp {
			delete(tdetail.CDR, hr)
		}
	}
	for hr := range odetail.CDR {
		if hr <= oexp {
			delete(odetail.CDR, hr)
		}
	}
	tsum, _ := m.data[0].(*termSum), m.data[1].(*origSum)
	for hr := range tsum.ByTo {
		if hr <= texp {
			delete(tsum.ByTo, hr)
		}
	}
	m.rel <- token
}
func cdraspMaint(n string) {
	m := mMod[n]
	goAfter(0, 60*time.Second, func() { gopher(n, m, cdraspInsert) })
	goAfter(240*time.Second, 270*time.Second, func() { cdraspClean(m) })
	goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
	for g, cl, fl :=
		time.NewTicker(360*time.Second),
		time.NewTicker(21600*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-g.C:
			goAfter(0, 60*time.Second, func() { gopher(n, m, cdraspInsert) })
		case <-cl.C:
			goAfter(240*time.Second, 270*time.Second, func() { cdraspClean(m) })
		case <-fl.C:
			goAfter(300*time.Second, 330*time.Second, func() { flush(n, m, 0, true) })
		}
	}
}
func cdraspTerm(n string, ctl chan string) {
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
