package main

import (
	"encoding/gob"
	"encoding/json"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
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
		Month map[string]*[2]int32           // month strings to hour ranges map
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

func load(n string) {
	var list []string
	if fn := settings.Models[n]; fn == "" {
		logE.Fatalf("no resource configured into which %q state may persist", n)
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
		} else if m := mMod[n]; err != nil {
			logE.Fatalf("cannot load %q state from %q: %v", n, fn, err)
		} else if pdata := m.data[0:m.persist]; strings.HasSuffix(fn, ".json") {
			dec := json.NewDecoder(f)
			err = dec.Decode(&pdata)
		} else {
			dec := gob.NewDecoder(f)
			err = dec.Decode(&pdata)
		}
		if f.Close(); err != nil {
			logE.Fatalf("%q state resource %q is invalid JSON/GOB: %v", n, fn, err)
		}
		return
	}
	logW.Printf("no %q state found at %q", n, list[0])
}

func persist(n string, final bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	if final {
		m.reqP <- acc
	} else {
		m.reqR <- acc
	}
	token, fn := <-acc, settings.Models[n]

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
		logE.Printf("can't persist %q state to %q: %v", n, fn, err)
		pr.CloseWithError(err)
	} else if _, err = io.Copy(f, pr); err != nil {
		logE.Printf("disruption persisting %q state to %q: %v", n, fn, err)
		pr.CloseWithError(err)
		f.Close()
	} else {
		f.Close()
	}
	if !final {
		m.rel <- token
	}
}

// trig.cmon model core accessors
//
func trigcmonBoot(n string) {
	trig, m := &trigModel{}, mMod[n]
	m.data = append(m.data, trig)
	m.persist = len(m.data)
	load(n)
}
func trigcmonClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
	token := <-acc

	// clean expired/invalid/insignificant data
	// trig := m.data[0].(*trigModel)
	m.rel <- token
	evt <- n
}
func trigcmonMaint(n string) {
	goaftSession(318*time.Second, 320*time.Second, func() { trigcmonClean(n, true) })
	goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

	for m, cl, p := mMod[n],
		time.NewTicker(3600*time.Second), time.NewTicker(10800*time.Second); ; {
		select {
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { trigcmonClean(n, true) })
		case <-p.C:
			goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

		case evt := <-m.evt:
			// events signal potentially non-meaningful updates (like cleans)
			goaftSession(0, 0, func() { trigcmonScan(n, evt) })
		}
	}
}
func trigcmonTerm(n string) {
	trigcmonClean(n, false)
	persist(n, true)
}

// *.aws model accessor helpers
//
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
func ec2awsBoot(n string) {
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
	load(n)

	if err := work.rates.Load(nil, "EC2"); err != nil {
		logE.Fatalf("%q cannot load EC2 rates: %v", n, err)
	}
	m.data = append(m.data, work)
}
func ec2awsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
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
	goaftSession(0, 18*time.Second, func() { gopher(n, ec2awsInsert, false) })
	goaftSession(318*time.Second, 320*time.Second, func() { ec2awsClean(n, true) })
	goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

	for m, g, sg, cl, p := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(3600*time.Second), time.NewTicker(7200*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 18*time.Second, func() { gopher(n, ec2awsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 18*time.Second, func() {gopher(n+"/stats", ec2awsSInsert)})
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { ec2awsClean(n, true) })
		case <-p.C:
			goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func ec2awsTerm(n string) {
	ec2awsClean(n, false)
	persist(n, true)
}

// ebs.aws model core accessors
//
func ebsawsBoot(n string) {
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
	load(n)

	if err := work.rates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", n, err)
	}
	m.data = append(m.data, work)
}
func ebsawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
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
	goaftSession(0, 18*time.Second, func() { gopher(n, ebsawsInsert, false) })
	goaftSession(318*time.Second, 320*time.Second, func() { ebsawsClean(n, true) })
	goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

	for m, g, sg, cl, p := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(3600*time.Second), time.NewTicker(7200*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 18*time.Second, func() { gopher(n, ebsawsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 18*time.Second, func() {gopher(n+"/stats", ebsawsSInsert)})
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { ebsawsClean(n, true) })
		case <-p.C:
			goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func ebsawsTerm(n string) {
	ebsawsClean(n, false)
	persist(n, true)
}

// rds.aws model core accessors
//
func rdsawsBoot(n string) {
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
	load(n)

	work.srates.Default = aws.DefaultRDSEBSRates
	if err := work.rates.Load(nil, "RDS"); err != nil {
		logE.Fatalf("%q cannot load RDS rates: %v", n, err)
	} else if err = work.srates.Load(nil); err != nil {
		logE.Fatalf("%q cannot load EBS rates: %v", n, err)
	}
	m.data = append(m.data, work)
}
func rdsawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
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
	goaftSession(0, 18*time.Second, func() { gopher(n, rdsawsInsert, false) })
	goaftSession(318*time.Second, 320*time.Second, func() { rdsawsClean(n, true) })
	goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

	for m, g, sg, cl, p := mMod[n],
		time.NewTicker(360*time.Second), time.NewTicker(7200*time.Second),
		time.NewTicker(3600*time.Second), time.NewTicker(7200*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 18*time.Second, func() { gopher(n, rdsawsInsert, false) })
		case <-sg.C:
			//goaftSession(0, 18*time.Second, func() {gopher(n+"/stats", rdsawsSInsert)})
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { rdsawsClean(n, true) })
		case <-p.C:
			goaftSession(328*time.Second, 332*time.Second, func() { persist(n, false) })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func rdsawsTerm(n string) {
	rdsawsClean(n, false)
	persist(n, true)
}

// cur.aws model core accessors
//
func curawsBoot(n string) {
	sum, detail, work, m := &curSum{
		ByAcct:   make(hsA, 2184),
		ByRegion: make(hsA, 2184),
		ByTyp:    make(hsA, 2184),
		BySvc:    make(hsA, 2184),
	}, &curDetail{
		Month: make(map[string]*[2]int32, 6),
		Line:  make(map[string]map[string]*curItem, 6),
	}, &curWork{}, mMod[n]
	m.data = append(m.data, sum)
	m.data = append(m.data, detail)
	m.persist = len(m.data)
	load(n)

	m.data = append(m.data, work)
}
func curawsClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
	token := <-acc

	// clean expired/invalid/insignificant data
	sum, detail := m.data[0].(*curSum), m.data[1].(*curDetail)
	exp := sum.Current - 24*90
	sum.ByAcct.clean(exp)
	sum.ByRegion.clean(exp)
	sum.ByTyp.clean(exp)
	sum.BySvc.clean(exp)
	for min := "9"; len(detail.Month) > 0; delete(detail.Month, min) {
		for mo := range detail.Month {
			if mo < min {
				min = mo
			}
		}
		hrs := detail.Month[min]
		for ; hrs[0] <= hrs[1] && sum.ByAcct[hrs[0]] == nil; hrs[0]++ {
		}
		if hrs[0] <= hrs[1] {
			break
		}
	}

	m.rel <- token
	evt <- n
}
func curawsMaint(n string) {
	goGo := make(chan bool, 1)
	goaftSession(0, 6*time.Second, func() {
		if gopher(n, curawsInsert, true) > 0 {
			curawsClean(n, true)
			persist(n, false)
		}
		goGo <- true
	})

	for m, g := mMod[n],
		time.NewTicker(720*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 6*time.Second, func() {
				select {
				case <-goGo: // serialize cur.aws gophers
					if gopher(n, curawsInsert, true) > 0 {
						curawsClean(n, true)
						persist(n, false)
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
func curawsTerm(n string) {
	acc := make(chan accTok, 1)
	mMod[n].reqP <- acc
	<-acc
}

// *.asp model accessor helpers
//
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

// cdr.asp model core accessors
//
func cdraspBoot(n string) {
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
	load(n)

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
}
func cdraspClean(n string, deep bool) {
	m, acc := mMod[n], make(chan accTok, 1)
	m.reqW <- acc
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
	goaftSession(0, 18*time.Second, func() { gopher(n, cdraspInsert, false); goGo <- true })
	goaftSession(318*time.Second, 320*time.Second, func() { cdraspClean(n, <-goGo); goGo <- true })
	goaftSession(328*time.Second, 332*time.Second, func() { persist(n, !<-goGo); goGo <- true })

	for m, g, cl, p := mMod[n],
		time.NewTicker(360*time.Second),
		time.NewTicker(1800*time.Second), time.NewTicker(14400*time.Second); ; {
		select {
		case <-g.C:
			goaftSession(0, 18*time.Second, func() {
				select {
				case <-goGo: // serialize cdr.asp gophers
					gopher(n, cdraspInsert, false)
					goGo <- true
				default:
				}
			})
		case <-cl.C:
			goaftSession(318*time.Second, 320*time.Second, func() { cdraspClean(n, <-goGo); goGo <- true })
		case <-p.C:
			goaftSession(328*time.Second, 332*time.Second, func() { persist(n, !<-goGo); goGo <- true })

		case <-m.evt:
			// TODO: process event notifications
		}
	}
}
func cdraspTerm(n string) {
	cdraspClean(n, false)
	persist(n, true)
}
