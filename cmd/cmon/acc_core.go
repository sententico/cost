package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/tel"
)

type (
	trigItem struct {
		Name   string
		Snap   map[string][][]string
		Action []string
	}
	trigModel struct {
		Placeholder string
		Trigger     []*trigItem

		tMap map[int64]*trigItem
	}

	perfItem struct {
		Period []int     `json:"P"`
		Value  []float32 `json:"V"`
	}
	usageItem struct {
		Usage uint64  `json:"U"` // total unit-seconds of usage
		Cost  float64 `json:"C"` // total USD cost (15-digit precision)
	}
	hsU map[int32]map[string]*usageItem // usage by hour/string descriptor
	hsA map[int32]map[string]float64    // amount (USD cost) by hour/string descriptor

	ec2Sum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsU   // map by hour / account
		ByRegion hsU   // map by hour / region
		BySKU    hsU   // map by hour / region+type+platform
	}
	ec2Item struct {
		Acct   string
		Typ    string
		Plat   string            `json:",omitempty"`
		Vol    int               `json:",omitempty"`
		AZ     string            `json:",omitempty"`
		AMI    string            `json:",omitempty"`
		Spot   string            `json:",omitempty"`
		Tag    map[string]string `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int                `json:",omitempty"`
		Perf   map[string]*perfItem `json:",omitempty"`
		Rate   float32              `json:",omitempty"`
	}
	ec2Detail struct {
		Current int
		Inst    map[string]*ec2Item
	}
	ec2Work struct {
		rates aws.Rater
	}

	ebsSum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsU   // map by hour / account
		ByRegion hsU   // map by hour / region
		BySKU    hsU   // map by hour / region+type
	}
	ebsItem struct {
		Acct   string
		Typ    string
		Size   int               `json:",omitempty"`
		IOPS   int               `json:",omitempty"`
		AZ     string            `json:",omitempty"`
		Mount  string            `json:",omitempty"`
		Tag    map[string]string `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int                `json:",omitempty"`
		Perf   map[string]*perfItem `json:",omitempty"`
		Rate   float32              `json:",omitempty"`
	}
	ebsDetail struct {
		Current int
		Vol     map[string]*ebsItem
	}
	ebsWork struct {
		rates aws.EBSRater
	}

	rdsSum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsU   // map by hour / account
		ByRegion hsU   // map by hour / region
		BySKU    hsU   // map by hour / region+type+engine
	}
	rdsItem struct {
		Acct    string
		Typ     string
		STyp    string            `json:",omitempty"`
		Size    int               `json:",omitempty"`
		IOPS    int               `json:",omitempty"`
		Engine  string            `json:",omitempty"`
		Ver     string            `json:",omitempty"`
		Lic     string            `json:",omitempty"`
		AZ      string            `json:",omitempty"`
		MultiAZ bool              `json:",omitempty"`
		Tag     map[string]string `json:",omitempty"`
		State   string
		Since   int
		Last    int
		Active  []int                `json:",omitempty"`
		Perf    map[string]*perfItem `json:",omitempty"`
		Rate    float32              `json:",omitempty"`
	}
	rdsDetail struct {
		Current int
		DB      map[string]*rdsItem
	}
	rdsWork struct {
		rates  aws.Rater
		srates aws.EBSRater
	}

	curSum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsA   // map by hour / account
		ByRegion hsA   // map by hour / region
		ByTyp    hsA   // map by hour / line item type
		BySvc    hsA   // map by hour / AWS service
	}
	curItem struct {
		Acct string    `json:"A"`
		Typ  string    `json:"T"`
		Svc  string    `json:"S,omitempty"`
		UTyp string    `json:"UT,omitempty"`
		UOp  string    `json:"UO,omitempty"`
		Reg  string    `json:"L,omitempty"`
		RID  string    `json:"R,omitempty"`
		Desc string    `json:"De,omitempty"`
		Name string    `json:"N,omitempty"`
		Env  string    `json:"E,omitempty"`
		DC   string    `json:"D,omitempty"`
		Prod string    `json:"P,omitempty"`
		App  string    `json:"Ap,omitempty"`
		Cust string    `json:"Cu,omitempty"`
		Team string    `json:"Te,omitempty"`
		Ver  string    `json:"V,omitempty"`
		Hour []uint32  `json:"H,omitempty"`  // type | range hrs (+base) | base (hrs in Unix epoch)
		HUsg []float32 `json:"HU,omitempty"` // hourly usage (maps to Hour ranges)
		Mu   int16     `json:"M,omitempty"`  // multiple CSV usage record count (+initial)
		Usg  float32   `json:"U,omitempty"`
		Cost float32   `json:"C,omitempty"`
	}
	curDetail struct {
		Month map[string][2]int32            // month strings to hour ranges map
		Line  map[string]map[string]*curItem // CUR line item map by month
	}
	curWork struct {
		imo   string              // CUR insertion month
		ihr   uint32              // CUR insertion default hour range (in Unix epoch)
		isum  curSum              // CUR summary insertion maps
		idet  curDetail           // CUR line item insertion map
		idetm map[string]*curItem // CUR line item month insertion map
	}

	callsItem struct {
		Calls uint32  `json:"N"`  // total number of calls (high-order 4 bits unused)
		Dur   uint64  `json:"D"`  // total 0.1s actual duration (high-order 24 bits unused)
		Bill  float64 `json:"TB"` // total billable USD (accumulated 4-digit rounded amounts)
		Marg  float64 `json:"TM"` // total margin USD (accumulated 6-digit rounded amounts)
	}
	cdrID   uint64
	cdrItem struct {
		To   tel.E164digest `json:"T"`           // decoded to number
		From tel.E164digest `json:"F,omitempty"` // decoded from number
		Cust string         `json:"C,omitempty"` // customer account/app
		Time uint32         `json:"S"`           // real duration (0.1s) | begin hour seconds offset
		Bill float32        `json:"B"`           // billable USD (rounded to 4 digits)
		Marg float32        `json:"M"`           // margin USD (rounded to 6 digits)
		Info uint16         `json:"I"`           // other info: loc code | tries (orig=0) | svc provider code
	}
	hsC     map[int32]map[string]*callsItem         // calls by hour/string descriptor
	hnC     map[int32]map[tel.E164digest]*callsItem // calls by hour/E.164 digest number
	hiD     map[int32]map[cdrID]*cdrItem            // CDRs (details) by hour/ID
	termSum struct {
		Current int32 // hour cursor in term summary maps (hours in Unix epoch)
		ByCust  hsC   // map by hour / customer (acct/app)
		ByGeo   hsC   // map by hour / to geo zone
		BySP    hsC   // map by hour / service provider
		ByLoc   hsC   // map by hour (hours in Unix epoch) / service location
		ByTo    hnC   // map by hour / to prefix (CC+P)
		ByFrom  hnC   // map by hour / full from number
	}
	origSum struct {
		Current int32 // hour cursor in orig summary maps (hours in Unix epoch)
		ByCust  hsC   // map by hour / customer (acct/app)
		ByGeo   hsC   // map by hour / from geo zone
		BySP    hsC   // map by hour / service provider
		ByLoc   hsC   // map by hour (hours in Unix epoch) / service location
		ByTo    hnC   // map by hour / full to number
		ByFrom  hnC   // map by hour / from prefix (CC+P)
	}
	termDetail struct {
		Current int32 // hour cursor in term CDR map (hours in Unix epoch)
		CDR     hiD   // map by hour/CDR ID
	}
	origDetail struct {
		Current int32 // hour cursor in orig CDR map (hours in Unix epoch)
		CDR     hiD   // map by hour/CDR ID
	}
	cdrWork struct {
		decoder, nadecoder     tel.Decoder    // CDR insertion decoders
		tbratesNA, tcratesNA   tel.Rater      // CDR insertion raters
		tbratesEUR, tcratesEUR tel.Rater      // CDR insertion raters
		obrates, ocrates       tel.Rater      // CDR insertion raters
		sp                     tel.SPmap      // CDR insertion service provider map
		sl                     tel.SLmap      // CDR insertion service location map
		to, fr                 tel.E164full   // CDR insertion decoder variable
		except                 map[string]int // CDR insertion exceptions map
	}
)

const (
	curItemMin = 0.05 // minimum CUR line item cost to keep hourly usage detail

	typShift   = 32 - 2   // CUR hour range type
	rangeShift = 30 - 10  // CUR hour range width (hours - 1)
	rangeMask  = 0x3ff    // CUR hour range width (hours - 1)
	baseMask   = 0xf_ffff // CUR hour range base (hours in Unix epoch)

	gwlocShift = 64 - 12            // CDR ID gateway loc (added to Ribbon ID for global uniqueness)
	shelfShift = 52 - 4             // CDR ID shelf (GSX could have 6)
	shelfMask  = 0xf                // CDR ID shelf (GSX could have 6)
	bootShift  = 48 - 16            // CDR ID boot sequence number
	bootMask   = 0xffff             // CDR ID boot sequence number
	callMask   = 0xffff_ffff        // CDR ID call sequence number
	idMask     = 0xf_ffff_ffff_ffff // CDR ID (Ribbon value without added location)

	durShift = 32 - 20 // CDR Time actual duration (0.1s)
	offMask  = 0xfff   // CDR Time call begin-hour offset (s)

	locShift   = 16 - 6 // CDR Info location code
	triesShift = 10 - 4 // CDR Info tries (0 for origination calls)
	triesMask  = 0xf    // CDR Info tries (0 for origination calls)
	spMask     = 0x3f   // CDR Info service provider code
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

func gopher(src string, insert func(*model, map[string]string, int), meta bool) {
	m, goph, eb, start, now := mMod[src], unleash(src), bytes.Buffer{}, int(time.Now().Unix()), 0
	gophStdout := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	acc, token, pages, items := make(chan accTok, 1), accTok(0), 0, 0
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
			if !meta {
				m.req <- modRq{atEXCL, acc}
				token = <-acc
				insert(m, nil, start)
				m.rel <- token
			}
			evt <- src
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
			if meta || item["~meta"] == "" {
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

func sync(n string) {
	if m, fn := mMod[n], settings.Models[n]; fn == "" {
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

func flush(n string, at accTyp, release bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{at, acc}
	token := <-acc

	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		enc.SetIndent("", "\t")
		enc.SetEscapeHTML(false)
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

// trig.cmon model core accessors
//
func trigcmonBoot(n string, ctl chan string) {
	trig, m := &trigModel{}, mMod[n]
	m.data = append(m.data, trig)
	m.persist = len(m.data)
	sync(n)
	ctl <- n
}
func trigcmonClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	// trig := m.data[0].(*trigModel)
	m.rel <- token
	evt <- n
}
func trigcmonScan(n string, evt string) {
	// TODO: process triggers on event; recover() wrapper; release models before other accesses
}
func trigcmonMaint(n string) {
	goaftSession(240*time.Second, 270*time.Second, func() { trigcmonClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, cl, fl := mMod[n],
		time.NewTicker(10800*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { trigcmonClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case evt := <-m.evt:
			goaftSession(0, 0, func() { trigcmonScan(n, evt) })
		}
	}
}
func trigcmonTerm(n string, ctl chan string) {
	trigcmonClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

func (m hsU) add(hr int32, k string, usage uint64, cost float32) {
	if hm := m[hr]; hm == nil {
		hm = make(map[string]*usageItem)
		m[hr], hm[k] = hm, &usageItem{Usage: usage, Cost: float64(cost)}
	} else if u := hm[k]; u == nil {
		hm[k] = &usageItem{Usage: usage, Cost: float64(cost)}
	} else {
		u.Usage += usage
		u.Cost += float64(cost)
	}
}
func (m hsU) clean(exp int32) {
	for hr := range m {
		if hr <= exp {
			delete(m, hr)
		}
	}
}
func (m hsA) add(hr int32, k string, amount float64) {
	if hm := m[hr]; hm == nil {
		hm = make(map[string]float64)
		m[hr], hm[k] = hm, amount
	} else {
		hm[k] += amount
	}
}
func (m hsA) update(u hsA) {
	for hr, hm := range u {
		for s, a := range hm {
			if ra := math.Round(a * 1e5); ra == 0 {
				delete(hm, s)
			} else {
				hm[s] = ra / 1e5
			}
		}
		m[hr] = hm
	}
}
func (m hsA) clean(exp int32) {
	for hr := range m {
		if hr <= exp {
			delete(m, hr)
		}
	}
}

// ec2.aws model core accessors
//
func ec2awsBoot(n string, ctl chan string) {
	sum, detail, work, m := &ec2Sum{
		ByAcct:   make(hsU, 2184),
		ByRegion: make(hsU, 2184),
		BySKU:    make(hsU, 2184),
	}, &ec2Detail{
		Inst: make(map[string]*ec2Item, 512),
	}, &ec2Work{}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	sync(n)

	if err := work.rates.Load(nil, "EC2"); err != nil {
		logE.Fatalf("%q cannot load EC2 rates: %v", n, err)
	}
	m.data = append(m.data, work)
	ctl <- n
}
func ec2awsHack(inst *ec2Item) {
	switch settings.Unit {
	case "cmon-aspect":
		// 88% of "sqlserver-se" EC2 instances identified by this hack (Oct20)
		if inst.Plat == "windows" && inst.Vol > 4 && (strings.HasSuffix(inst.Tag["app"], ".edw") ||
			strings.HasSuffix(inst.Tag["app"], ".wfd") ||
			strings.HasSuffix(inst.Tag["app"], "_db")) {
			inst.Plat = "sqlserver-se"
		}
	}
}
func ec2awsInsert(m *model, item map[string]string, now int) {
	sum, detail, work, id := m.data[0].(*ec2Sum), m.data[1].(*ec2Detail), m.data[2].(*ec2Work), item["id"]
	if item == nil {
		if now > detail.Current {
			detail.Current = now
		}
		return
	} else if id == "" {
		return
	}
	inst := detail.Inst[id]
	if inst == nil {
		inst = &ec2Item{
			Typ:   item["type"],
			AMI:   item["ami"],
			Since: now,
		}
		detail.Inst[id] = inst
	}
	inst.Acct = item["acct"]
	inst.Plat = item["plat"]
	inst.Vol = atoi(item["vol"], 0)
	inst.AZ = item["az"]
	inst.Spot = item["spot"]
	if tag := item["tag"]; tag != "" {
		inst.Tag = make(map[string]string)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			inst.Tag[kvs[0]] = kvs[1]
		}
	} else {
		inst.Tag = nil
	}

	ec2awsHack(inst)
	k := aws.RateKey{
		Region: inst.AZ,
		Typ:    inst.Typ,
		Plat:   inst.Plat,
		Terms:  "OD",
	}
	switch inst.State = item["state"]; inst.State {
	case "running":
		if r := work.rates.Lookup(&k); r.Rate == 0 {
			logE.Printf("no EC2 %v rate found for %v/%v in %v", k.Terms, k.Typ, k.Plat, inst.AZ)
		} else if inst.Spot == "" {
			k.Terms = settings.AWS.SavPlan
			if s := work.rates.Lookup(&k); s.Rate == 0 {
				inst.Rate = r.Rate * settings.AWS.UsageAdj
			} else {
				inst.Rate = (r.Rate*(1-settings.AWS.SavCov) + s.Rate*settings.AWS.SavCov) * settings.AWS.UsageAdj
			}
		} else {
			inst.Rate = r.Rate * (1 - settings.AWS.SpotDisc) * settings.AWS.UsageAdj
		}
		if inst.Active == nil || inst.Last > inst.Active[len(inst.Active)-1] {
			inst.Active = append(inst.Active, now, now)
		} else {
			inst.Active[len(inst.Active)-1] = now
			dur := uint64(now - inst.Last)
			hr, c := int32(now/3600), inst.Rate*float32(dur)/3600
			if hr > sum.Current {
				sum.Current = hr
			}
			sum.ByAcct.add(hr, inst.Acct, dur, c)
			sum.ByRegion.add(hr, k.Region, dur, c)
			if inst.Spot == "" {
				sum.BySKU.add(hr, k.Region+" "+k.Typ+" "+k.Plat, dur, c)
			} else {
				sum.BySKU.add(hr, k.Region+" sp."+k.Typ+" "+k.Plat, dur, c)
			}
		}
	default:
		inst.Rate = 0
	}
	inst.Last = now
}
func ec2awsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*ec2Sum), m.data[1].(*ec2Detail)
	for id, inst := range detail.Inst {
		if id == "" || detail.Current-inst.Last > 86400*8 {
			delete(detail.Inst, id)
		}
	}
	exp := sum.Current - 24*90
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	m.rel <- token
	evt <- n
}
func ec2awsMaint(n string) {
	goaftSession(0, 60*time.Second, func() { gopher(n, ec2awsInsert, false) })
	goaftSession(240*time.Second, 270*time.Second, func() { ec2awsClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, g, sg, cl, fl := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(10800*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 60*time.Second, func() { gopher(n, ec2awsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 60*time.Second, func() {gopher(n+"/stats", ec2awsSInsert)})
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { ec2awsClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	ec2awsClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

// ebs.aws model core accessors
//
func ebsawsBoot(n string, ctl chan string) {
	sum, detail, work, m := &ebsSum{
		ByAcct:   make(hsU, 2184),
		ByRegion: make(hsU, 2184),
		BySKU:    make(hsU, 2184),
	}, &ebsDetail{
		Vol: make(map[string]*ebsItem, 1024),
	}, &ebsWork{}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	sync(n)

	if err := work.rates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", n, err)
	}
	m.data = append(m.data, work)
	ctl <- n
}
func ebsawsInsert(m *model, item map[string]string, now int) {
	sum, detail, work, id := m.data[0].(*ebsSum), m.data[1].(*ebsDetail), m.data[2].(*ebsWork), item["id"]
	if item == nil {
		if now > detail.Current {
			detail.Current = now
		}
		return
	} else if id == "" {
		return
	}
	vol, dur := detail.Vol[id], 0
	if vol == nil {
		vol = &ebsItem{
			Typ:   item["type"],
			Since: now,
		}
		detail.Vol[id] = vol
	} else {
		dur = now - vol.Last
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

	k, c := aws.EBSRateKey{
		Region: vol.AZ,
		Typ:    vol.Typ,
	}, float32(0)
	r := work.rates.Lookup(&k)
	if r.SZrate == 0 {
		logE.Printf("no EBS rate found for %v in %v", k.Typ, vol.AZ)
	}
	switch vol.State = item["state"]; vol.State {
	case "in-use":
		if vol.Active == nil || vol.Last > vol.Active[len(vol.Active)-1] {
			vol.Active = append(vol.Active, now, now)
		} else {
			vol.Active[len(vol.Active)-1] = now
		}
		fallthrough
	case "available":
		vol.Rate, c = (r.SZrate*float32(vol.Size)+r.IOrate*float32(vol.IOPS))*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
	default:
		vol.Rate = 0
	}
	if c > 0 {
		hr, u := int32(now/3600), uint64(vol.Size*dur)
		if hr > sum.Current {
			sum.Current = hr
		}
		sum.ByAcct.add(hr, vol.Acct, u, c)
		sum.ByRegion.add(hr, k.Region, u, c)
		sum.BySKU.add(hr, k.Region+" "+k.Typ, u, c)
	}
	vol.Last = now
}
func ebsawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*ebsSum), m.data[1].(*ebsDetail)
	for id, vol := range detail.Vol {
		if id == "" || detail.Current-vol.Last > 86400*8 {
			delete(detail.Vol, id)
		}
	}
	exp := sum.Current - 24*90
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	m.rel <- token
	evt <- n
}
func ebsawsMaint(n string) {
	goaftSession(0, 60*time.Second, func() { gopher(n, ebsawsInsert, false) })
	goaftSession(240*time.Second, 270*time.Second, func() { ebsawsClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, g, sg, cl, fl := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(10800*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 60*time.Second, func() { gopher(n, ebsawsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 60*time.Second, func() {gopher(n+"/stats", ebsawsSInsert)})
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { ebsawsClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func ebsawsTerm(n string, ctl chan string) {
	ebsawsClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

// rds.aws model core accessors
//
func rdsawsBoot(n string, ctl chan string) {
	sum, detail, work, m := &rdsSum{
		ByAcct:   make(hsU, 2184),
		ByRegion: make(hsU, 2184),
		BySKU:    make(hsU, 2184),
	}, &rdsDetail{
		DB: make(map[string]*rdsItem, 128),
	}, &rdsWork{}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	sync(n)

	work.srates.Default = aws.DefaultRDSEBSRates
	if err := work.rates.Load(nil, "RDS"); err != nil {
		logE.Fatalf("%q cannot load RDS rates: %v", n, err)
	} else if err = work.srates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", n, err)
	}
	m.data = append(m.data, work)
	ctl <- n
}
func rdsawsInsert(m *model, item map[string]string, now int) {
	sum, detail, work, id := m.data[0].(*rdsSum), m.data[1].(*rdsDetail), m.data[2].(*rdsWork), item["id"]
	if item == nil {
		if now > detail.Current {
			detail.Current = now
		}
		return
	} else if id == "" {
		return
	}
	db, dur, az := detail.DB[id], 0, 1
	if db == nil {
		db = &rdsItem{
			Typ:    item["type"],
			STyp:   item["stype"],
			Engine: item["engine"],
			Since:  now,
		}
		detail.DB[id] = db
	} else {
		dur = now - db.Last
	}
	db.Acct = item["acct"]
	db.Size = atoi(item["size"], 0)
	db.IOPS = atoi(item["iops"], 0)
	db.Ver = item["ver"]
	db.AZ = item["az"]
	db.Lic = item["lic"]
	if db.MultiAZ = item["multiaz"] == "True"; db.MultiAZ {
		az = 2
	}
	if tag := item["tag"]; tag != "" {
		db.Tag = make(map[string]string)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			db.Tag[kvs[0]] = kvs[1]
		}
	} else {
		db.Tag = nil
	}

	k := aws.RateKey{
		Region: db.AZ,
		Typ:    db.Typ,
		Plat:   db.Engine,
		Terms:  "OD",
	}
	r, s, u, c := work.rates.Lookup(&k), work.srates.Lookup(&aws.EBSRateKey{
		Region: db.AZ,
		Typ:    db.STyp,
	}), uint64(0), float32(0)
	if r.Rate == 0 || s.SZrate == 0 {
		logE.Printf("no RDS %v rate found for %v/%v[%v] in %v", k.Terms, k.Typ, k.Plat, db.STyp, db.AZ)
	}
	switch db.State = item["state"]; db.State {
	case "available", "backing-up":
		if db.Active == nil || db.Last > db.Active[len(db.Active)-1] {
			db.Active = append(db.Active, now, now)
		} else {
			db.Active[len(db.Active)-1], u = now, uint64(az*dur)
		}
		db.Rate, c = (r.Rate*float32(az)+s.SZrate*float32(az*db.Size)+s.IOrate*float32(az*db.IOPS))*settings.AWS.UsageAdj, db.Rate*float32(dur)/3600
	case "stopped", "stopping":
		db.Rate = (s.SZrate*float32(az*db.Size) + s.IOrate*float32(az*db.IOPS)) * settings.AWS.UsageAdj
		c = db.Rate * float32(dur) / 3600
	default:
		db.Rate = 0
	}
	if c > 0 {
		hr := int32(now / 3600)
		if hr > sum.Current {
			sum.Current = hr
		}
		sum.ByAcct.add(hr, db.Acct, u, c)
		sum.ByRegion.add(hr, k.Region, u, c)
		sum.BySKU.add(hr, k.Region+" "+k.Typ+" "+k.Plat, u, c)
	}
	db.Last = now
}
func rdsawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*rdsSum), m.data[1].(*rdsDetail)
	for id, db := range detail.DB {
		if id == "" || detail.Current-db.Last > 86400*8 {
			delete(detail.DB, id)
		}
	}
	exp := sum.Current - 24*90
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	m.rel <- token
	evt <- n
}
func rdsawsMaint(n string) {
	goaftSession(0, 60*time.Second, func() { gopher(n, rdsawsInsert, false) })
	goaftSession(240*time.Second, 270*time.Second, func() { rdsawsClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, g, sg, cl, fl := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(10800*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 60*time.Second, func() { gopher(n, rdsawsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 60*time.Second, func() {gopher(n+"/stats", rdsawsSInsert)})
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { rdsawsClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	rdsawsClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

// cur.aws model core accessors
//
func curawsBoot(n string, ctl chan string) {
	sum, detail, work, m := &curSum{
		ByAcct:   make(hsA, 2184),
		ByRegion: make(hsA, 2184),
		ByTyp:    make(hsA, 2184),
		BySvc:    make(hsA, 2184),
	}, &curDetail{
		Month: make(map[string][2]int32, 6),
		Line:  make(map[string]map[string]*curItem, 6),
	}, &curWork{}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	sync(n)

	m.data = append(m.data, work)
	ctl <- n
}
func curawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	sum := m.data[0].(*curSum)
	exp := sum.Current - 24*90
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.ByTyp.clean(exp)
	sum.BySvc.clean(exp)

	m.rel <- token
	evt <- n
}
func curawsInsert(m *model, item map[string]string, now int) {
	work, id := m.data[2].(*curWork), item["id"]
	if id == "" {
		if meta := item["~meta"]; strings.HasPrefix(meta, "begin ") {
			work.imo, work.isum, work.idet, work.idetm = "", curSum{
				ByAcct:   make(hsA),
				ByRegion: make(hsA),
				ByTyp:    make(hsA),
				BySvc:    make(hsA),
			}, curDetail{
				Line: make(map[string]map[string]*curItem),
			}, nil
		} else if strings.HasPrefix(meta, "section 20") && len(meta) > 14 {
			if t, err := time.Parse(time.RFC3339, meta[8:12]+"-"+meta[12:14]+"-01T00:00:00Z"); err == nil {
				if work.imo = meta[8:14]; work.idet.Line[work.imo] == nil {
					work.idet.Line[work.imo] = make(map[string]*curItem)
				}
				work.ihr = uint32(t.Unix()/3600) & baseMask
				work.idetm = work.idet.Line[work.imo]
			} else {
				work.imo = ""
				logE.Printf("unrecognized AWS CUR input: %q", meta[8:])
			}
		} else if strings.HasPrefix(meta, "end ") {
			for _, m := range work.idet.Line {
				for id, line := range m {
					if line.Cost == 0 {
						delete(m, id)
					} else if line.Cost < curItemMin && -curItemMin < line.Cost {
						line.Hour, line.HUsg = nil, nil
					}
				}
			}
			psum, pdet := m.data[0].(*curSum), m.data[1].(*curDetail)
			psum.ByAcct.update(work.isum.ByAcct)
			psum.ByRegion.update(work.isum.ByRegion)
			psum.ByTyp.update(work.isum.ByTyp)
			psum.BySvc.update(work.isum.BySvc)
			for mo := range work.idet.Line {
				pdet.Line[mo] = work.idet.Line[mo]
				bt, _ := time.Parse(time.RFC3339, mo[:4]+"-"+mo[4:]+"-01T00:00:00Z")
				bh, eh := int32(bt.Unix())/3600, int32(bt.AddDate(0, 1, 0).Unix()-1)/3600
				for ; eh > bh && psum.ByAcct[eh] == nil; eh-- {
				}
				if pdet.Month[mo] = [2]int32{bh, eh}; eh > psum.Current {
					psum.Current = eh
				}
			}
			mos, exp := 0, work.imo
			for mo := range pdet.Line {
				if mos++; mo < exp {
					exp = mo
				}
			}
			if work.idet.Line = nil; mos > 3 {
				delete(pdet.Line, exp)
			}
		}
		return
	} else if work.imo == "" {
		return
	}

	h := work.ihr
	if t, err := time.Parse(time.RFC3339, item["hour"]); err == nil {
		h = uint32(t.Unix()/3600) & baseMask
	}
	u, _ := strconv.ParseFloat(item["usg"], 64)
	c, _ := strconv.ParseFloat(item["cost"], 64)
	line, hr, us, co := work.idetm[id], int32(h), float32(u), float32(c)
	if line == nil {
		line = &curItem{
			Acct: item["acct"],
			Typ:  item["typ"],
			Svc:  item["svc"],
			UTyp: item["utyp"],
			UOp:  item["uop"],
			Reg:  item["reg"],
			RID:  item["rid"],
			Desc: item["desc"],
			Name: item["name"],
			Env:  item["env"],
			DC:   item["dc"],
			Prod: item["prod"],
			App:  item["app"],
			Cust: item["cust"],
			Team: item["team"],
			Ver:  item["ver"],
			Hour: []uint32{h},
			HUsg: []float32{us},
		}
		work.idetm[id] = line
	} else if last := line.Hour[len(line.Hour)-1]; us == line.HUsg[len(line.HUsg)-1] &&
		h == (last&baseMask)+(last>>rangeShift&rangeMask)+1 {
		line.Hour[len(line.Hour)-1] += 1 << rangeShift
		line.Mu++
	} else {
		line.Hour = append(line.Hour, h)
		line.HUsg = append(line.HUsg, us)
		line.Mu++
	}
	line.Usg += us
	line.Cost += co
	work.isum.ByAcct.add(hr, line.Acct, c)
	work.isum.ByRegion.add(hr, line.Reg, c)
	work.isum.ByTyp.add(hr, line.Typ, c)
	work.isum.BySvc.add(hr, line.Svc, c)
}
func curawsMaint(n string) {
	goGo := make(chan bool, 1)
	goaftSession(0, 60*time.Second, func() {
		gopher(n, curawsInsert, true)
		goGo <- true
	})
	goaftSession(240*time.Second, 270*time.Second, func() { curawsClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, g, cl, fl := mMod[n],
		time.NewTicker(360*time.Second),
		time.NewTicker(21600*time.Second), time.NewTicker(21600*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 60*time.Second, func() {
				select {
				case <-goGo: // serialize cur.aws gophers
					gopher(n, curawsInsert, true)
					goGo <- true
				default:
				}
			})
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { curawsClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func curawsTerm(n string, ctl chan string) {
	curawsClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

// cdr.asp model core accessors
//
func cdraspBoot(n string, ctl chan string) {
	tsum, osum, tdetail, odetail, work, m := &termSum{
		ByCust: make(hsC, 2184),
		ByGeo:  make(hsC, 2184),
		BySP:   make(hsC, 2184),
		ByLoc:  make(hsC, 2184),
		ByTo:   make(hnC, 2184),
		ByFrom: make(hnC, 2184),
	}, &origSum{
		ByCust: make(hsC, 2184),
		ByGeo:  make(hsC, 2184),
		BySP:   make(hsC, 2184),
		ByLoc:  make(hsC, 2184),
		ByTo:   make(hnC, 2184),
		ByFrom: make(hnC, 2184),
	}, &termDetail{
		CDR: make(hiD, 60),
	}, &origDetail{
		CDR: make(hiD, 60),
	}, &cdrWork{
		except: make(map[string]int),
	}, mMod[n]
	m.data = append(m.data, tsum)
	m.data = append(m.data, osum)
	m.data = append(m.data, tdetail)
	m.data = append(m.data, odetail)
	m.persist = len(m.data)
	sync(n)

	work.nadecoder.NANPbias = true
	work.tbratesNA.Default, work.tcratesNA.Default = tel.DefaultTermBillNA, tel.DefaultTermCostNA
	work.tbratesNA.DefaultRate, work.tcratesNA.DefaultRate = 0.01, 0.005
	work.tbratesEUR.Default, work.tcratesEUR.Default = tel.DefaultTermBillNA, tel.DefaultTermCostNA // TODO: tel.DefaultTermBillEUR, tel.DefaultTermCostEUR
	work.tbratesEUR.DefaultRate, work.tcratesEUR.DefaultRate = 0.02, 0.01
	work.obrates.Default, work.ocrates.Default = tel.DefaultOrigBill, tel.DefaultOrigCost
	work.obrates.DefaultRate, work.ocrates.DefaultRate = 0.006, 0.002
	if err := work.decoder.Load(nil); err != nil {
		logE.Fatalf("%q cannot load E.164 decoder: %v", n, err)
	} else if err = work.nadecoder.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NANP-biased E.164 decoder: %v", n, err)
	} else if err = work.tbratesNA.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NA termination bill rates: %v", n, err)
	} else if err = work.tcratesNA.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NA termination cost rates: %v", n, err)
	} else if err = work.tbratesEUR.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EUR termination bill rates: %v", n, err)
	} else if err = work.tcratesEUR.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EUR termination cost rates: %v", n, err)
	} else if err = work.obrates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load origination bill rates: %v", n, err)
	} else if err = work.ocrates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load origination cost rates: %v", n, err)
	} else if err = work.sp.Load(nil); err != nil {
		logE.Fatalf("%q cannot load service provider map: %v", n, err)
	} else if err = work.sl.Load(nil); err != nil {
		logE.Fatalf("%q cannot load service location map: %v", n, err)
	}
	m.data = append(m.data, work)
	ctl <- n
}
func (id cdrID) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(strconv.FormatUint(uint64(id), 16))), nil
}
func (id *cdrID) UnmarshalText(b []byte) error {
	if x, err := strconv.ParseUint(string(b), 16, 64); err != nil {
		return err
	} else {
		*id = cdrID(x)
		return nil
	}
}
func (m hiD) add(hr int32, id cdrID, cdr *cdrItem) bool {
	if hm := m[hr]; hm == nil {
		hm = make(map[cdrID]*cdrItem, 4096)
		m[hr], hm[id] = hm, cdr
	} else if hm[id] == nil {
		hm[id] = cdr
	} else {
		return false
	}
	return true
}
func (m hsC) add(hr int32, k string, cdr *cdrItem) {
	if hm := m[hr]; hm == nil {
		hm = make(map[string]*callsItem)
		m[hr], hm[k] = hm, &callsItem{Calls: 1, Dur: uint64(cdr.Time >> durShift), Bill: float64(cdr.Bill), Marg: float64(cdr.Marg)}
	} else if c := hm[k]; c == nil {
		hm[k] = &callsItem{Calls: 1, Dur: uint64(cdr.Time >> durShift), Bill: float64(cdr.Bill), Marg: float64(cdr.Marg)}
	} else {
		c.Calls++
		c.Dur += uint64(cdr.Time >> durShift)
		c.Bill += float64(cdr.Bill)
		c.Marg += float64(cdr.Marg)
	}
}
func (m hsC) clean(exp int32) {
	for hr := range m {
		if hr <= exp {
			delete(m, hr)
		}
	}
}
func (m hsC) sig(active int32, insig string, min float64) {
	for hr, sm := range m {
		if ic := sm[insig]; ic == nil && hr < active {
			ic = &callsItem{}
			for s, sc := range sm {
				if min > sc.Bill && min > sc.Marg && sc.Marg > -min {
					ic.Calls += sc.Calls
					ic.Dur += sc.Dur
					ic.Bill += sc.Bill
					ic.Marg += sc.Marg
					delete(sm, s)
				}
			}
			sm[insig] = ic
		}
	}
}
func (m hnC) add(hr int32, k tel.E164digest, cdr *cdrItem) {
	if hm := m[hr]; hm == nil {
		hm = make(map[tel.E164digest]*callsItem)
		m[hr], hm[k] = hm, &callsItem{Calls: 1, Dur: uint64(cdr.Time >> durShift), Bill: float64(cdr.Bill), Marg: float64(cdr.Marg)}
	} else if c := hm[k]; c == nil {
		hm[k] = &callsItem{Calls: 1, Dur: uint64(cdr.Time >> durShift), Bill: float64(cdr.Bill), Marg: float64(cdr.Marg)}
	} else {
		c.Calls++
		c.Dur += uint64(cdr.Time >> durShift)
		c.Bill += float64(cdr.Bill)
		c.Marg += float64(cdr.Marg)
	}
}
func (m hnC) clean(exp int32) {
	for hr := range m {
		if hr <= exp {
			delete(m, hr)
		}
	}
}
func (m hnC) sig(active int32, min float64) {
	for hr, nm := range m {
		if ic := nm[0]; ic == nil && hr < active {
			ic = &callsItem{}
			for n, nc := range nm {
				if min > nc.Bill && min > nc.Marg && nc.Marg > -min {
					ic.Calls += nc.Calls
					ic.Dur += nc.Dur
					ic.Bill += nc.Bill
					ic.Marg += nc.Marg
					delete(nm, n)
				}
			}
			nm[0] = ic
		}
	}
}
func billmarg(brate float32, crate float32, dur uint32) (b float32, m float32) {
	if r := dur % 60; r > 0 {
		m = float32(dur+60-r) / 600
	} else {
		m = float32(dur) / 600
	}
	if b = m; dur < 300 {
		b = 0.5
	}
	b *= brate
	return float32(math.Round(float64(b)*1e4) / 1e4), float32(math.Round(float64(b-m*crate)*1e6) / 1e6)
}
func cdraspInsert(m *model, item map[string]string, now int) {
	id, tsum, osum := cdrID(ato64(item["id"], 0)), m.data[0].(*termSum), m.data[1].(*origSum)
	tdetail, odetail, work := m.data[2].(*termDetail), m.data[3].(*origDetail), m.data[4].(*cdrWork)
	b, err := time.Parse(time.RFC3339, item["begin"])
	if err != nil || id == 0 {
		if len(work.except) > 0 {
			logE.Printf("\"cdr.asp\" insertion exceptions: %v", work.except)
			work.except = make(map[string]int)
		}
		return
	}
	beg, dur, lc := int32(b.Unix()), uint32(atoi(item["dur"], 0)+9)/10, work.sl.Code(item["loc"])
	cdr, hr, methods := &cdrItem{
		Cust: item["cust"],
		Time: dur<<durShift | uint32(beg%3600),
		Info: lc << locShift,
	}, beg/3600, func(in bool) (d *tel.Decoder, br *tel.Rater, cr *tel.Rater) {
		switch work.sl.Name(lc) {
		case "ASH", "LAS", "lab", "AWS lab":
			if d = &work.nadecoder; in {
				br, cr = &work.obrates, &work.ocrates
			} else {
				br, cr = &work.tbratesNA, &work.tcratesNA
			}
		default:
			if d = &work.decoder; in {
				br, cr = &work.obrates, &work.ocrates
			} else {
				br, cr = &work.tbratesEUR, &work.tcratesEUR
			}
		}
		return
	}

	switch typ, ip, tg := item["type"], item["IP"], item["iTG"]; {
	case typ == "CORE" || tg == "USPRODMBZ_ZIPWIRE_TG":
		// agent/ignored CDR
	case typ == "CARRIER" || len(ip) > 3 && ip[:3] == "10.":
		// inbound/origination CDR
		decoder, brater, crater := methods(true)
		if decoder.Full(item["to"], &work.to) != nil {
			break
		}
		decoder.Full(item["from"], &work.fr)
		cdr.To, cdr.From = work.to.Digest(0), work.fr.Digest(0)
		cdr.Bill, cdr.Marg = billmarg(brater.Lookup(&work.to), crater.Lookup(&work.to), dur)
		if len(tg) > 6 && tg[:6] == "ASPTIB" {
			cdr.Info |= work.sp.Code(tg[6:]) & spMask
		} else if len(tg) > 5 && tg[:5] == "SUAIB" {
			cdr.Info |= work.sp.Code(tg[5:]) & spMask
		} else if len(tg) > 4 { // BYOC/PBXC
			cdr.Info |= work.sp.Code(tg[:4]) & spMask
		}
		if odetail.CDR.add(hr, cdrID(lc)<<gwlocShift|id&idMask, cdr) {
			if cdr.Info&spMask == 0 {
				work.except["iTG:"+tg]++
			}
			if hr > osum.Current {
				osum.Current, odetail.Current = hr, hr
			}
			osum.ByCust.add(hr, cdr.Cust, cdr)
			osum.BySP.add(hr, work.sp.Name(cdr.Info&spMask), cdr)
			osum.ByLoc.add(hr, work.sl.Name(lc), cdr)
			osum.ByTo.add(hr, cdr.To, cdr)
			if cdr.From != 0 {
				osum.ByGeo.add(hr, work.fr.Geo, cdr)
				osum.ByFrom.add(hr, work.fr.Digest(len(work.fr.CC)+len(work.fr.P)), cdr)
			}
		}
	default:
		// outbound/termination CDR
		decoder, brater, crater := methods(false)
		if len(item["dip"]) >= 20 && decoder.Full(item["dip"][:10], &work.to) != nil ||
			decoder.Full(item["to"], &work.to) != nil {
			break
		}
		cdr.To, cdr.From = work.to.Digest(0), decoder.Digest(item["from"])
		if crate, err := strconv.ParseFloat(item["rate"], 32); err == nil {
			cdr.Bill, cdr.Marg = billmarg(brater.Lookup(&work.to), float32(crate), dur)
		} else {
			cdr.Bill, cdr.Marg = billmarg(brater.Lookup(&work.to), crater.Lookup(&work.to), dur)
		}
		if tries := uint16(atoi(item["try"], 1)); tries > triesMask {
			cdr.Info |= triesMask << triesShift
		} else {
			cdr.Info |= tries << triesShift
		}
		if tg = item["eTG"]; len(tg) > 6 && tg[:6] == "ASPTOB" {
			cdr.Info |= work.sp.Code(tg[6:]) & spMask
		} else if len(tg) > 4 { // BYOC/PBXC
			cdr.Info |= work.sp.Code(tg[:4]) & spMask
		}
		if tdetail.CDR.add(hr, cdrID(lc)<<gwlocShift|id&idMask, cdr) {
			if cdr.Info&spMask == 0 {
				work.except["eTG:"+tg]++
			}
			if hr > tsum.Current {
				tsum.Current, tdetail.Current = hr, hr
			}
			tsum.ByCust.add(hr, cdr.Cust, cdr)
			tsum.ByGeo.add(hr, work.to.Geo, cdr)
			tsum.BySP.add(hr, work.sp.Name(cdr.Info&spMask), cdr)
			tsum.ByLoc.add(hr, work.sl.Name(lc), cdr)
			tsum.ByTo.add(hr, work.to.Digest(len(work.to.CC)+len(work.to.P)), cdr)
			if cdr.From != 0 {
				tsum.ByFrom.add(hr, cdr.From, cdr)
			}
		}
	}
}
func cdraspClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.req <- modRq{atEXCL, acc}
	token := <-acc

	// clean expired/invalid/insignificant data
	tdetail, odetail := m.data[2].(*termDetail), m.data[3].(*origDetail)
	texp, oexp := tdetail.Current-27, odetail.Current-27
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
	tsum, osum := m.data[0].(*termSum), m.data[1].(*origSum)
	tsum.ByCust.sig(texp, "other", 1.00)
	tsum.ByTo.sig(texp, 1.00)
	tsum.ByFrom.sig(texp, 1.00)
	osum.ByCust.sig(oexp, "other", 1.00)
	osum.ByTo.sig(oexp, 1.00)
	osum.ByFrom.sig(oexp, 1.00)
	texp, oexp = tsum.Current-24*90, osum.Current-24*90
	tsum.ByCust.clean(texp)
	tsum.ByGeo.clean(texp)
	tsum.BySP.clean(texp)
	tsum.ByLoc.clean(texp)
	tsum.ByTo.clean(texp)
	tsum.ByFrom.clean(texp)
	osum.ByCust.clean(oexp)
	osum.ByGeo.clean(oexp)
	osum.BySP.clean(oexp)
	osum.ByLoc.clean(oexp)
	osum.ByTo.clean(oexp)
	osum.ByFrom.clean(oexp)

	m.rel <- token
	evt <- n
}
func cdraspMaint(n string) {
	goGo := make(chan bool, 1)
	goaftSession(0, 60*time.Second, func() {
		gopher(n, cdraspInsert, false)
		goGo <- true
	})
	goaftSession(240*time.Second, 270*time.Second, func() { cdraspClean(n, true) })
	goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

	for m, g, cl, fl := mMod[n],
		time.NewTicker(360*time.Second),
		time.NewTicker(21600*time.Second), time.NewTicker(21600*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 60*time.Second, func() {
				select {
				case <-goGo: // serialize cdr.asp gophers
					gopher(n, cdraspInsert, false)
					goGo <- true
				default:
				}
			})
		case <-cl.C:
			goaftSession(240*time.Second, 270*time.Second, func() { cdraspClean(n, true) })
		case <-fl.C:
			goaftSession(300*time.Second, 320*time.Second, func() { flush(n, 0, true) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func cdraspTerm(n string, ctl chan string) {
	cdraspClean(n, false)
	flush(n, atEXCL, false)
	ctl <- n
}

func atoi(s string, d int) int {
	if i, err := strconv.ParseInt(s, 0, 0); err == nil {
		return int(i)
	}
	return d
}
func ato64(s string, d uint64) uint64 {
	if i, err := strconv.ParseUint(s, 0, 64); err == nil {
		return i
	}
	return d
}
