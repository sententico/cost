package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
	"github.com/sententico/cost/cmon"
	"github.com/sententico/cost/tel"
)

type (
	cmdMap map[string]string

	evtModel struct {
		Placeholder string
		Alert       map[string]map[string]string
	}

	usageItem struct {
		Usage uint64  `json:"U"` // total unit-seconds of usage
		Cost  float64 `json:"C"` // total USD cost (15-digit precision)
	}
	hsU map[int32]map[string]*usageItem // usage by hour/string descriptor
	hsA map[int32]map[string]float64    // amount (USD cost) by hour/string descriptor
	riO map[string][2]int32             // observation range (hours in Unix epoch) by resource ID

	ec2Sum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsU   // map by hour / account
		ByRegion hsU   // map by hour / region
		BySKU    hsU   // map by hour / region+type+platform
	}
	ec2Item struct {
		Acct   string
		Typ    string
		Plat   string      `json:",omitempty"`
		Vol    int         `json:",omitempty"`
		AZ     string      `json:",omitempty"`
		VPC    string      `json:",omitempty"`
		AMI    string      `json:",omitempty"`
		Spot   string      `json:",omitempty"`
		Tag    cmon.TagMap `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int          `json:",omitempty"`
		Metric cmon.MetricMap `json:",omitempty"`
		ORate  float32        `json:",omitempty"`
		Rate   float32        `json:",omitempty"`
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
		GiB    int         `json:",omitempty"`
		IOPS   int         `json:",omitempty"`
		MiBps  int         `json:",omitempty"`
		AZ     string      `json:",omitempty"`
		Mount  string      `json:",omitempty"`
		Tag    cmon.TagMap `json:",omitempty"`
		State  string
		Since  int
		Last   int
		Active []int          `json:",omitempty"`
		Metric cmon.MetricMap `json:",omitempty"`
		Rate   float32        `json:",omitempty"`
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
		STyp    string      `json:",omitempty"`
		GiB     int         `json:",omitempty"`
		IOPS    int         `json:",omitempty"`
		Engine  string      `json:",omitempty"`
		Ver     string      `json:",omitempty"`
		Lic     string      `json:",omitempty"`
		AZ      string      `json:",omitempty"`
		MultiAZ bool        `json:",omitempty"`
		VPC     string      `json:",omitempty"`
		Tag     cmon.TagMap `json:",omitempty"`
		State   string
		Since   int
		Last    int
		Active  []int          `json:",omitempty"`
		Metric  cmon.MetricMap `json:",omitempty"`
		ORate   float32        `json:",omitempty"`
		Rate    float32        `json:",omitempty"`
	}
	rdsDetail struct {
		Current int
		DB      map[string]*rdsItem
	}
	rdsWork struct {
		rates  aws.Rater
		srates aws.EBSRater
	}

	snapSum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsU   // map by hour / account
		ByRegion hsU   // map by hour / region
	}
	snapItem struct {
		Acct  string      `json:"A"`
		Typ   string      `json:"T,omitempty"`
		Size  float32     `json:"S,omitempty"`
		VSiz  int         `json:"Vs,omitempty"`
		Reg   string      `json:"L"`
		Vol   string      `json:"V,omitempty"` // volume root of snapshot tree
		Par   string      `json:"P,omitempty"` // parent snapshot
		Desc  string      `json:"D,omitempty"`
		Tag   cmon.TagMap `json:"Tg,omitempty"`
		Since int         `json:"Si"`
		Last  int         `json:"La"`
		Rate  float32     `json:"R,omitempty"`
	}
	snapDetail struct {
		Current int
		Snap    map[string]*snapItem
	}
	snapWork struct {
		rates aws.SnapRater
	}

	curSum struct {
		Current  int32 // hour cursor in summary maps (hours in Unix epoch)
		ByAcct   hsA   // map by hour / account
		ByRegion hsA   // map by hour / region
		ByTyp    hsA   // map by hour / line item type
		BySvc    hsA   // map by hour / AWS service
		Hist     riO   // map range of observation by resource ID
	}
	curItem struct {
		Acct string    `json:"A"`
		Typ  string    `json:"T"`
		Svc  string    `json:"S,omitempty"`
		UTyp string    `json:"UT,omitempty"`
		UOp  string    `json:"UO,omitempty"`
		Reg  string    `json:"L,omitempty"`
		RID  string    `json:"I,omitempty"`
		Desc string    `json:"D,omitempty"`
		Name string    `json:"N,omitempty"`
		Env  string    `json:"E,omitempty"`
		Prod string    `json:"P,omitempty"`
		Role string    `json:"Ro,omitempty"`
		Ver  string    `json:"V,omitempty"`
		Prov string    `json:"Pv,omitempty"`
		Oper string    `json:"O,omitempty"`
		Bill string    `json:"B,omitempty"`
		Cust string    `json:"Cu,omitempty"`
		HMap []uint32  `json:"H,omitempty"`  // range hrs (+base) | usage (index/value) | base (mo-hr offset) [alt: 0b110 | Recs-offset bit-map]
		HUsg []float32 `json:"HU,omitempty"` // hourly usage (offset by Recs from mo-hr or indexed by HMap)
		Recs uint32    `json:"R,omitempty"`  // CUR records (count-1) | from | to (mo-hr offsets)
		Usg  float32   `json:"U"`
		Chg  float32   `json:"C"`
	}
	curDetail struct {
		Month map[string]*[2]int32           // month strings to hour ranges map
		Line  map[string]map[string]*curItem // CUR line item map by month
	}
	curWork struct {
		imo   string              // CUR insertion month
		ihr   int32               // CUR insertion month base hour (in Unix epoch)
		isum  curSum              // CUR summary insertion maps
		idet  curDetail           // CUR line item insertion map
		idetm map[string]*curItem // CUR line item month insertion map (reference to idet entry)
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
		except, dexcept        map[string]int // CDR insertion exceptions map
	}
)

const (
	fetchCycle = 360 // base/standard fetch cycle (seconds)
)

func (m *model) load() {
	var list []string
	if fn := settings.Models[m.name]; fn == "" {
		logE.Fatalf("no resource configured into which %q state may persist", m.name)
	} else if strings.HasSuffix(fn, ".json") {
		list = []string{fn, fn[:len(fn)-5] + ".gob", fn[:len(fn)-5]}
	} else if strings.HasSuffix(fn, ".gob") {
		list = []string{fn, fn[:len(fn)-4] + ".json", fn[:len(fn)-4]}
	} else {
		list = []string{fn, fn + ".json", fn + ".gob"}
	}
	var f *os.File
	var err error
	for _, fn := range list {
		if f, err = os.Open(fn); os.IsNotExist(err) {
			continue
		} else if err != nil {
			logE.Fatalf("cannot load %q state from %q: %v", m.name, fn, err)
		} else if pdata := m.data[0:m.persist]; strings.HasSuffix(fn, ".json") {
			dec := json.NewDecoder(f)
			err = dec.Decode(&pdata)
		} else {
			dec := gob.NewDecoder(f)
			err = dec.Decode(&pdata)
		}
		if f.Close(); err != nil {
			logE.Fatalf("%q state resource %q is invalid JSON/GOB: %v", m.name, fn, err)
		}
		return
	}
	logW.Printf("no %q state found at %q", m.name, list[0])
}

func (m *model) store(final bool) {
	acc, fn := m.newAcc(), settings.Models[m.name]
	if final {
		acc.reqP()
	} else {
		acc.reqRt(-1)
		defer acc.rel()
	}

	pr, pw := io.Pipe()
	go func() {
		var err error
		if pdata := m.data[0:m.persist]; strings.HasSuffix(fn, ".json") {
			enc := json.NewEncoder(pw)
			enc.SetIndent("", "\t")
			enc.SetEscapeHTML(false)
			err = enc.Encode(&pdata)
		} else {
			enc := gob.NewEncoder(pw)
			err = enc.Encode(&pdata)
		}
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	if f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664); err != nil {
		logE.Printf("can't store %q state in %q: %v", m.name, fn, err)
		pr.CloseWithError(err)
	} else if _, err = io.Copy(f, pr); err != nil {
		logE.Printf("disruption storing %q state in %q: %v", m.name, fn, err)
		pr.CloseWithError(err)
		f.Close()
		os.Remove(fn) // delete corrupt/incomplete state to forestall startup failure
	} else {
		f.Close()
	}
}

func (m cmdMap) new(key string, input []interface{}, opt ...string) (cin io.WriteCloser, cout io.ReadCloser, err error) {
	var cerr io.ReadCloser
	if cmd := func() *exec.Cmd {
		for suffix := strings.SplitN(key, "/", 2)[0]; ; suffix = suffix[1:] {
			if c := m[suffix]; c != "" {
				args := []string{
					"python3",
					fmt.Sprintf("%v/%v", strings.TrimRight(settings.BinDir, "/"), c),
				}
				// TODO: change to exec.CommandContext() to support timeouts?
				return exec.Command(args[0], append(append(args[1:], opt...), key)...)
			} else if suffix == "" {
				return nil
			}
		}
	}(); cmd == nil {
		err = fmt.Errorf("no %s found for %q", m["~"], key)
	} else if cin, err = cmd.StdinPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q %s: %v", key, m["~"], err)
	} else if cerr, err = cmd.StderrPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q %s: %v", key, m["~"], err)
	} else if cout, err = cmd.StdoutPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q %s: %v", key, m["~"], err)
		//} else if cout, cmd.Stdout = io.Pipe(); false {
		// ...this option nicely forces cmd.Wait() cleanup to synchronize with cout emptying/closure
		// ...but appears to interfere with bufio.Scanner EOF processing
	} else if err = cmd.Start(); err != nil {
		err = fmt.Errorf("%q %s refused release: %v", key, m["~"], err)
	} else if err = func() (err error) {
		if _, err = io.WriteString(cin, settings.JSON); err == nil && len(input) > 0 {
			enc := json.NewEncoder(cin)
			for _, obj := range input {
				if err = enc.Encode(obj); err != nil {
					return
				}
			}
		}
		return
	}(); err != nil {
		cin.Close()
		cout.Close()
		cerr.Close()
		cmd.Wait()
		err = fmt.Errorf("setup problem with %q %s: %v", key, m["~"], err)
	} else {
		go func() {
			var em string
			// wait for command to complete; invoking thread must close cin/cout
			if eb, _ := io.ReadAll(cerr); len(eb) > 0 {
				el := bytes.Split(bytes.Trim(eb, "\n\t "), []byte("\n"))
				em = fmt.Sprintf(" [%s]", bytes.TrimLeft(el[len(el)-1], "\t "))
			} else {
				// give cout opportunity to be cleared (hinky)
				time.Sleep(250 * time.Millisecond)
			}
			if e := cmd.Wait(); e != nil {
				switch e {
				case io.ErrClosedPipe:
					logE.Printf("%q %s reply abandoned%s", key, m["~"], em)
				default:
					logE.Printf("%q %s errors: %v%s", key, m["~"], e, em)
				}
			} else if len(em) > 0 {
				logE.Printf("%q %s warnings:%s", key, m["~"], em)
			}
		}()

		return
	}
	return nil, nil, err
}

// evt.cmon model core accessors...
func evtcmonBoot(m *model) {
	evt := &evtModel{
		Alert: make(map[string]map[string]string, 512),
	}
	m.data = append(m.data, evt)
	m.persist = len(m.data)
	m.load()
}
func evtcmonClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	evt := m.data[0].(*evtModel)
	for id, a := range evt.Alert {
		if rst, _ := time.Parse(time.RFC3339, a["reset"]); time.Until(rst.Add(time.Hour*24*180)) < 0 {
			delete(evt.Alert, id)
		}
	}

	acc.rel()
}
func evtcmonMaint(m *model) {
	goaftSession(318*time.Second, 320*time.Second, func() { evtcmonClean(m, true) })
	goaftSession(328*time.Second, 332*time.Second, func() { m.store(false) })

	for cl, st :=
		time.NewTicker(fetchCycle*10*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { evtcmonClean(m, true) })
		case <-st.C:
			goaftSession(328*time.Second, 332*time.Second, func() { m.store(false) })

		case event := <-m.evt:
			goaftSession(0, 0, func() { evtcmonHandler(m, event) })
		}
	}
}
func evtcmonTerm(m *model) {
	evtcmonClean(m, false)
	m.store(true)
}

// *.aws model accessor helpers...
func (m hsU) add(now, dur int, k string, usage uint64, cost float32) (hr int32) {
	var pu uint64
	var p, pc, fd, fu, fc float64
	if hr = int32(now / 3600); dur > 3600*24*180 {
		dur = 3600 * 24 * 180
	}
	for h, rd, hc := hr, dur, now%3600+1; rd > 0; h, rd, hc = h-1, rd-hc, 3600 {
		// proportion usage/cost since connectivity disruptions may cause long durations
		if hc >= rd {
			if rd == dur {
				pu, pc = usage, float64(cost)
			} else {
				p = float64(rd) / fd
				pu, pc = uint64(fu*p+0.5), fc*p
			}
		} else {
			if fd == 0 {
				fd, fu, fc = float64(dur), float64(usage), float64(cost)
			}
			p = float64(hc) / fd
			pu, pc = uint64(fu*p+0.5), fc*p
		}

		if hm := m[h]; hm == nil {
			hm = make(map[string]*usageItem)
			m[h], hm[k] = hm, &usageItem{Usage: pu, Cost: pc}
		} else if u := hm[k]; u == nil {
			hm[k] = &usageItem{Usage: pu, Cost: pc}
		} else {
			u.Usage += pu
			u.Cost += pc
		}
	}
	return
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
func (m hsA) update(u hsA, from, to int32) {
	for hr, hm := range u {
		if from <= hr && hr <= to {
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
}
func (m hsA) clean(exp int32) {
	for hr := range m {
		if hr <= exp {
			delete(m, hr)
		}
	}
}
func (m riO) update(acc *modAcc, mo map[string]*curItem, from, to int32) {
	if match, pg := settings.AWS.CUR["HistMatch"], lgPage; match != "" {
		re, _ := regexp.Compile(match) // filter history by RID match to regex/prefix criteria
		for _, li := range mo {
			if li.RID == "" || re == nil && !strings.HasPrefix(li.RID, match) {
			} else if func() int {
				if pg--; pg < 0 { // pagination provides cooperative access and avoids write token expiration
					acc.rel()
					pg = lgPage
					acc.reqW()
				}
				return pg
			}() < 0 || re != nil && !re.MatchString(li.RID) {
			} else if o, f, t := m[li.RID], int32(li.Recs>>foffShift&foffMask)+from, int32(li.Recs&toffMask)+from; o[1] == 0 {
				m[li.RID] = [2]int32{f, t}
			} else if t > o[1] {
				if o[1] = t; f < o[0] {
					o[0] = f
				}
				m[li.RID] = o
			} else if f < o[0] {
				o[0] = f
				m[li.RID] = o
			}
		}
	}
}
func (m riO) ppuse(ri string, from, to int32) float32 {
	switch o := m[ri]; {
	case o[1] == 0:
	case o[1] < from:
		return -1
	case o[0] > to:
		return 1
	case o[0] <= from && o[1] >= to:

	case o[0] < from: // post-use part of from/to period (<0, usage ending)
		return -1 + float32(o[1]-from+1)/float32(to-from+1)
	case o[1] > to: // pre-use part of from/to period (>0, usage starting)
		return 1 - float32(to-o[0]+1)/float32(to-from+1)
	}
	return 0
}
func (m riO) clean(short, long int32) {
	for ri, o := range m {
		if o[1] <= long || o[1]-o[0] < 4 && o[1] <= short {
			delete(m, ri)
		}
	}
}

// ec2.aws model core accessors...
func ec2awsBoot(m *model) {
	sum, detail, work := &ec2Sum{
		ByAcct:   make(hsU, 24*181),
		ByRegion: make(hsU, 24*181),
		BySKU:    make(hsU, 24*181),
	}, &ec2Detail{
		Inst: make(map[string]*ec2Item, 512),
	}, &ec2Work{}
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	m.load()

	if err := work.rates.Load(nil, "EC2"); err != nil {
		logE.Fatalf("%q cannot load EC2 rates: %v", m.name, err)
	}
	m.data = append(m.data, work)
}
func ec2awsClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*ec2Sum), m.data[1].(*ec2Detail)
	for id, inst := range detail.Inst {
		if x := detail.Current - inst.Last; inst.State == "terminated" && inst.Last-inst.Since < 72*3600 &&
			x > 3*fetchCycle || x > 72*3600 {
			delete(detail.Inst, id)
			continue
		}
		for me, ts := range inst.Metric {
			if exp := len(ts) - 24*30; exp > 0 {
				inst.Metric[me] = ts[exp:]
			}
		}
	}
	exp := sum.Current - 24*180
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	acc.rel()
}
func ec2awsMaint(m *model) {
	modifySettings := func() {
		acc := m.newAcc()
		acc.reqR()
		defer func() {
			acc.rel()
			if r := recover(); r != nil {
				logE.Printf("cannot update %q settings: %v", m.name, r)
			}
		}()
		if _, err := cmon.Reload(&settings, func(new *cmon.MonSettings) (modify bool) {
			for _, inst := range m.data[1].(*ec2Detail).Inst {
				if new.PromoteAZ(inst.Acct, inst.AZ) {
					logW.Printf("%s AZ access promoted for account %s", inst.AZ, inst.Acct)
					modify = true
				}
			}
			return
		}); err != nil {
			panic(err)
		}
	}
	goaftSession(0, 18*time.Second, func() {
		if modifySettings(); fetch(m.newAcc(), "/metrics", ec2awsInsert, false) > 0 {
			ec2awsClean(m, true)
			modifySettings()
			evt <- new(modEvt).append(m.name)
		}
	})
	goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

	for f, fc, cl, st := time.NewTicker(fetchCycle*time.Second), uint32(0),
		time.NewTicker(10*fetchCycle*time.Second), time.NewTicker(20*fetchCycle*time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 18*time.Second, func() {
				if fetch(m.newAcc(), func() string {
					switch fc++; fc % 20 {
					case 0, 10:
						return "/metrics"
					}
					return ""
				}(), ec2awsInsert, false) > 0 {
					modifySettings()
					evt <- new(modEvt).append(m.name)
				}
			})
		case <-cl.C:
			goaftSession((fetchCycle-42)*time.Second, (fetchCycle-40)*time.Second, func() { ec2awsClean(m, true) })
		case <-st.C:
			goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

		case event := <-m.evt:
			switch event.name {
			case "settings":
				goaftSession(0, 0, func() { modifySettings() })
			case "cur.aws":
				goaftSession(0, 0, func() { ec2awsFeedback(m, event); evt <- event.append(m.name) })
			}
		}
	}
}
func ec2awsTerm(m *model) {
	ec2awsClean(m, false)
	m.store(true)
}

// ebs.aws model core accessors...
func ebsawsBoot(m *model) {
	sum, detail, work := &ebsSum{
		ByAcct:   make(hsU, 24*181),
		ByRegion: make(hsU, 24*181),
		BySKU:    make(hsU, 24*181),
	}, &ebsDetail{
		Vol: make(map[string]*ebsItem, 1024),
	}, &ebsWork{}
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	m.load()

	if err := work.rates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", m.name, err)
	}
	m.data = append(m.data, work)
}
func ebsawsClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*ebsSum), m.data[1].(*ebsDetail)
	for id, vol := range detail.Vol {
		if x := detail.Current - vol.Last; vol.Last-vol.Since < 12*3600 && x > 3*3600 || x > 72*3600 {
			delete(detail.Vol, id)
			continue
		}
		for me, ts := range vol.Metric {
			if exp := len(ts) - 24*30; exp > 0 {
				vol.Metric[me] = ts[exp:]
			}
		}
	}
	exp := sum.Current - 24*180
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	acc.rel()
}
func ebsawsMaint(m *model) {
	modifySettings := func() {
		acc := m.newAcc()
		acc.reqR()
		defer func() {
			acc.rel()
			if r := recover(); r != nil {
				logE.Printf("cannot update %q settings: %v", m.name, r)
			}
		}()
		if _, err := cmon.Reload(&settings, func(new *cmon.MonSettings) (modify bool) {
			for _, vol := range m.data[1].(*ebsDetail).Vol {
				if new.PromoteAZ(vol.Acct, vol.AZ) {
					logW.Printf("%s AZ access promoted for account %s", vol.AZ, vol.Acct)
					modify = true
				}
			}
			return
		}); err != nil {
			panic(err)
		}
	}
	goaftSession(0, 18*time.Second, func() {
		if modifySettings(); fetch(m.newAcc(), "/metrics", ebsawsInsert, false) > 0 {
			ebsawsClean(m, true)
			modifySettings()
			evt <- new(modEvt).append(m.name)
		}
	})
	goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

	for f, fc, cl, st := time.NewTicker(fetchCycle*time.Second), uint32(0),
		time.NewTicker(10*fetchCycle*time.Second), time.NewTicker(20*fetchCycle*time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 18*time.Second, func() {
				if fetch(m.newAcc(), func() string {
					switch fc++; fc % 20 {
					case 0, 10:
						return "/metrics"
					}
					return ""
				}(), ebsawsInsert, false) > 0 {
					modifySettings()
					evt <- new(modEvt).append(m.name)
				}
			})
		case <-cl.C:
			goaftSession((fetchCycle-42)*time.Second, (fetchCycle-40)*time.Second, func() { ebsawsClean(m, true) })
		case <-st.C:
			goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

		case event := <-m.evt:
			switch event.name {
			case "settings":
				goaftSession(0, 0, func() { modifySettings() })
			}
		}
	}
}
func ebsawsTerm(m *model) {
	ebsawsClean(m, false)
	m.store(true)
}

// rds.aws model core accessors...
func rdsawsBoot(m *model) {
	sum, detail, work := &rdsSum{
		ByAcct:   make(hsU, 24*181),
		ByRegion: make(hsU, 24*181),
		BySKU:    make(hsU, 24*181),
	}, &rdsDetail{
		DB: make(map[string]*rdsItem, 128),
	}, &rdsWork{}
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	m.load()

	work.srates.Default = aws.DefaultRDSEBSRates
	if err := work.rates.Load(nil, "RDS"); err != nil {
		logE.Fatalf("%q cannot load RDS rates: %v", m.name, err)
	} else if err = work.srates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", m.name, err)
	}
	m.data = append(m.data, work)
}
func rdsawsClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*rdsSum), m.data[1].(*rdsDetail)
	for id, db := range detail.DB {
		if x := detail.Current - db.Last; db.Last-db.Since < 12*3600 && x > 3*3600 || x > 72*3600 {
			delete(detail.DB, id)
			continue
		}
		for me, ts := range db.Metric {
			if exp := len(ts) - 24*30; exp > 0 {
				db.Metric[me] = ts[exp:]
			}
		}
	}
	exp := sum.Current - 24*180
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.BySKU.clean(exp)

	acc.rel()
}
func rdsawsMaint(m *model) {
	modifySettings := func() {
		acc := m.newAcc()
		acc.reqR()
		defer func() {
			acc.rel()
			if r := recover(); r != nil {
				logE.Printf("cannot update %q settings: %v", m.name, r)
			}
		}()
		if _, err := cmon.Reload(&settings, func(new *cmon.MonSettings) (modify bool) {
			for _, db := range m.data[1].(*rdsDetail).DB {
				if new.PromoteAZ(db.Acct, db.AZ) {
					logW.Printf("%s AZ access promoted for account %s", db.AZ, db.Acct)
					modify = true
				}
			}
			return
		}); err != nil {
			panic(err)
		}
	}
	goaftSession(0, 18*time.Second, func() {
		if modifySettings(); fetch(m.newAcc(), "/metrics", rdsawsInsert, false) > 0 {
			rdsawsClean(m, true)
			modifySettings()
			evt <- new(modEvt).append(m.name)
		}
	})
	goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

	for f, fc, cl, st := time.NewTicker(fetchCycle*time.Second), uint32(0),
		time.NewTicker(10*fetchCycle*time.Second), time.NewTicker(20*fetchCycle*time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 18*time.Second, func() {
				if fetch(m.newAcc(), func() string {
					switch fc++; fc % 20 {
					case 0, 10:
						return "/metrics"
					}
					return ""
				}(), rdsawsInsert, false) > 0 {
					modifySettings()
					evt <- new(modEvt).append(m.name)
				}
			})
		case <-cl.C:
			goaftSession((fetchCycle-42)*time.Second, (fetchCycle-40)*time.Second, func() { rdsawsClean(m, true) })
		case <-st.C:
			goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(false) })

		case event := <-m.evt:
			switch event.name {
			case "settings":
				goaftSession(0, 0, func() { modifySettings() })
			case "cur.aws":
				goaftSession(0, 0, func() { rdsawsFeedback(m, event); evt <- event.append(m.name) })
			}
		}
	}
}
func rdsawsTerm(m *model) {
	rdsawsClean(m, false)
	m.store(true)
}

// snap.aws model core accessors...
func snapawsBoot(m *model) {
	sum, detail, work := &snapSum{
		ByAcct:   make(hsU, 24*181),
		ByRegion: make(hsU, 24*181),
	}, &snapDetail{
		Snap: make(map[string]*snapItem, 4096),
	}, &snapWork{}
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	m.load()

	if err := work.rates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS snapshot rates: %v", m.name, err)
	}
	m.data = append(m.data, work)
}
func snapawsClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*snapSum), m.data[1].(*snapDetail)
	for id, snap := range detail.Snap {
		if x := detail.Current - snap.Last; snap.Last-snap.Since < 12*3600 && x > 3*3600 || x > 72*3600 {
			delete(detail.Snap, id)
		}
	}
	exp := sum.Current - 24*180
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)

	acc.rel()
}
func snapawsMaint(m *model) {
	modifySettings := func() {
		acc := m.newAcc()
		acc.reqR()
		defer func() {
			acc.rel()
			if r := recover(); r != nil {
				logE.Printf("cannot update %q settings: %v", m.name, r)
			}
		}()
		if _, err := cmon.Reload(&settings, func(new *cmon.MonSettings) (modify bool) {
			for _, snap := range m.data[1].(*snapDetail).Snap {
				if new.PromoteAZ(snap.Acct, snap.Reg) {
					logW.Printf("%s access promoted for account %s", snap.Reg, snap.Acct)
					modify = true
				}
			}
			return
		}); err != nil {
			panic(err)
		}
	}
	goaftSession(0, 18*time.Second, func() {
		if modifySettings(); fetch(m.newAcc(), "", snapawsInsert, false) > 0 {
			snapawsClean(m, true)
			modifySettings()
			evt <- new(modEvt).append(m.name)
		}
	})
	goaftSession((10*fetchCycle-32)*time.Second, (10*fetchCycle-28)*time.Second, func() { m.store(false) })

	for f, cl, st := time.NewTicker(10*fetchCycle*time.Second),
		time.NewTicker(20*fetchCycle*time.Second), time.NewTicker(20*fetchCycle*time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 18*time.Second, func() {
				if fetch(m.newAcc(), "", snapawsInsert, false) > 0 {
					modifySettings()
					evt <- new(modEvt).append(m.name)
				}
			})
		case <-cl.C:
			goaftSession((10*fetchCycle-42)*time.Second, (10*fetchCycle-40)*time.Second, func() { snapawsClean(m, true) })
		case <-st.C:
			goaftSession((10*fetchCycle-32)*time.Second, (10*fetchCycle-28)*time.Second, func() { m.store(false) })

		case event := <-m.evt:
			switch event.name {
			case "settings":
				goaftSession(0, 0, func() { modifySettings() })
			case "cur.aws":
				goaftSession(0, 0, func() { snapawsFeedback(m, event); evt <- event.append(m.name) })
			}
		}
	}
}
func snapawsTerm(m *model) {
	snapawsClean(m, false)
	m.store(true)
}

// cur.aws model core accessors...
func curawsBoot(m *model) {
	sum, detail, work := &curSum{
		ByAcct:   make(hsA, 24*181),
		ByRegion: make(hsA, 24*181),
		ByTyp:    make(hsA, 24*181),
		BySvc:    make(hsA, 24*181),
		Hist:     make(riO, 16384),
	}, &curDetail{
		Month: make(map[string]*[2]int32, 6),
		Line:  make(map[string]map[string]*curItem, 6),
	}, &curWork{}
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	m.load()

	m.data = append(m.data, work)
}
func curawsClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// remove expired detail months; trim month map hours & remove entries no longer needed
	sum, detail := m.data[0].(*curSum), m.data[1].(*curDetail)
	var sm []string
	for m := range detail.Month {
		sm = append(sm, m)
	}
	if sort.Strings(sm); len(sm) > 0 {
		hrs := detail.Month[sm[len(sm)-1]]
		for ; hrs[1] > hrs[0] && sum.ByTyp[hrs[1]] == nil; hrs[1]-- {
		}
		for sum.Current = hrs[1]; sum.Current > hrs[0] && sum.ByTyp[sum.Current]["usage"] == 0; sum.Current-- {
		}
		sum.Current -= 3 // account for unstable trailing hours of reported usage
		exp := sum.Current - 24*180
		sum.ByAcct.clean(exp)
		sum.ByRegion.clean(exp)
		sum.ByTyp.clean(exp)
		sum.BySvc.clean(exp)
		sum.Hist.clean(sum.Current-24*3, exp)
		if len(sm) > 4 {
			for _, m := range sm[:len(sm)-4] {
				for hrs = detail.Month[m]; hrs[0] <= hrs[1] && sum.ByTyp[hrs[0]] == nil; hrs[0]++ {
				}
				if delete(detail.Line, m); hrs[0] > hrs[1] {
					delete(detail.Month, m)
				}
			}
		}
	}

	acc.rel()
}
func curawsMaint(m *model) {
	goGo := make(chan bool, 1)
	goaftSession(0, 6*time.Second, func() {
		if fetch(m.newAcc(), "", curawsInsert, true) > 0 {
			curawsClean(m, true)
			m.store(false)
			evt <- new(modEvt).append(m.name)
		}
		goGo <- true
	})

	for f := time.NewTicker(6 * fetchCycle * time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 6*time.Second, func() {
				select {
				case <-goGo: // serialize cur.aws gophers
					if fetch(m.newAcc(), "", curawsInsert, true) > 0 {
						curawsClean(m, true)
						m.store(false)
						evt <- new(modEvt).append(m.name)
					}
					goGo <- true
				default:
				}
			})

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func curawsTerm(m *model) {
	m.newAcc().reqP()
}

// *.asp model accessor helpers...
func (id cdrID) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(strconv.FormatUint(uint64(id), 16))), nil
}
func (id *cdrID) UnmarshalText(b []byte) error {
	x, err := strconv.ParseUint(string(b), 16, 64)
	if err != nil {
		return err
	}
	*id = cdrID(x)
	return nil
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

// cdr.asp model core accessors...
func cdraspBoot(m *model) {
	tsum, osum, tdetail, odetail, work := &termSum{
		ByCust: make(hsC, 24*181),
		ByGeo:  make(hsC, 24*181),
		BySP:   make(hsC, 24*181),
		ByLoc:  make(hsC, 24*181),
		ByTo:   make(hnC, 24*181),
		ByFrom: make(hnC, 24*181),
	}, &origSum{
		ByCust: make(hsC, 24*181),
		ByGeo:  make(hsC, 24*181),
		BySP:   make(hsC, 24*181),
		ByLoc:  make(hsC, 24*181),
		ByTo:   make(hnC, 24*181),
		ByFrom: make(hnC, 24*181),
	}, &termDetail{
		CDR: make(hiD, 60),
	}, &origDetail{
		CDR: make(hiD, 60),
	}, &cdrWork{
		except:  make(map[string]int),
		dexcept: make(map[string]int, 4096),
	}
	m.data = append(m.data, tsum)
	m.data = append(m.data, osum)
	m.data = append(m.data, tdetail)
	m.data = append(m.data, odetail)
	m.persist = len(m.data)
	m.load()

	work.nadecoder.NANPbias = true
	work.tbratesNA.Default, work.tcratesNA.Default = tel.DefaultTermBillNA, tel.DefaultTermCostNA
	work.tbratesNA.DefaultRate, work.tcratesNA.DefaultRate = 0.01, 0.005
	work.tbratesEUR.Default, work.tcratesEUR.Default = tel.DefaultTermBillNA, tel.DefaultTermCostNA // TODO: tel.DefaultTermBillEUR, tel.DefaultTermCostEUR
	work.tbratesEUR.DefaultRate, work.tcratesEUR.DefaultRate = 0.02, 0.01
	work.obrates.Default, work.ocrates.Default = tel.DefaultOrigBill, tel.DefaultOrigCost
	work.obrates.DefaultRate, work.ocrates.DefaultRate = 0.006, 0.002
	if err := work.decoder.Load(nil); err != nil {
		logE.Fatalf("%q cannot load E.164 decoder: %v", m.name, err)
	} else if err = work.nadecoder.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NANP-biased E.164 decoder: %v", m.name, err)
	} else if err = work.tbratesNA.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NA termination bill rates: %v", m.name, err)
	} else if err = work.tcratesNA.Load(nil); err != nil {
		logE.Fatalf("%q cannot load NA termination cost rates: %v", m.name, err)
	} else if err = work.tbratesEUR.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EUR termination bill rates: %v", m.name, err)
	} else if err = work.tcratesEUR.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EUR termination cost rates: %v", m.name, err)
	} else if err = work.obrates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load origination bill rates: %v", m.name, err)
	} else if err = work.ocrates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load origination cost rates: %v", m.name, err)
	} else if err = work.sp.Load(nil); err != nil {
		logE.Fatalf("%q cannot load service provider map: %v", m.name, err)
	} else if err = work.sl.Load(nil); err != nil {
		logE.Fatalf("%q cannot load service location map: %v", m.name, err)
	}
	m.data = append(m.data, work)
}
func cdraspClean(m *model, deep bool) {
	acc := m.newAcc()
	acc.reqW()

	// clean expired/invalid/insignificant data
	tdetail, odetail := m.data[2].(*termDetail), m.data[3].(*origDetail)
	texp, oexp := tdetail.Current-40, odetail.Current-40
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
	texp, oexp = tsum.Current-24*180, osum.Current-24*180
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

	acc.rel()
}
func cdraspMaint(m *model) {
	goGo := make(chan bool, 1)
	goaftSession(0, 18*time.Second, func() {
		if fetch(m.newAcc(), "", cdraspInsert, false) > 0 {
			cdraspClean(m, true)
			evt <- new(modEvt).append(m.name)
		}
		goGo <- true
	})
	goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(!<-goGo); goGo <- true })

	for f, cl, st := time.NewTicker(fetchCycle*time.Second),
		time.NewTicker(5*fetchCycle*time.Second), time.NewTicker(40*fetchCycle*time.Second); ; {
		select {
		case <-f.C:
			goaftSession(0, 18*time.Second, func() {
				select {
				case <-goGo: // serialize cdr.asp gophers
					if fetch(m.newAcc(), "", cdraspInsert, false) > 0 {
						evt <- new(modEvt).append(m.name)
					}
					goGo <- true
				default:
				}
			})
		case <-cl.C:
			goaftSession((fetchCycle-42)*time.Second, (fetchCycle-40)*time.Second, func() { cdraspClean(m, <-goGo); goGo <- true })
		case <-st.C:
			goaftSession((fetchCycle-32)*time.Second, (fetchCycle-28)*time.Second, func() { m.store(!<-goGo); goGo <- true })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func cdraspTerm(m *model) {
	cdraspClean(m, false)
	m.store(true)
}
