package main

import (
	"fmt"
	"math"
	"net/url"
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
	varexVol struct {
		id    string
		stype string
		gib   float32
		iops  float32
		rate  float32
	}
	varexInst struct {
		id    string
		name  string
		itype string
		plat  string
		arate float32
		vols  map[string]*varexVol // map of mounts/devices to volumes
	}
	varexEnv struct {
		tref string
		reg  string                 // region locating environment resources (assumes 1)
		ec2  map[string][]varexInst // map of resource types to instances
	}
)

const (
	maxTableRows = 3e7 // maximum table extract rows allowed
	maxPctMargin = 10  // maximum magnitude of %margin for non-billed amounts
)

var (
	weaselCmd = cmdMap{
		"aws":   "wea_aws.py",
		"dd":    "wea_dd.py",
		"slack": "wea_slack.py",
		"twil":  "wea_twilio.py",
		"":      "wea_test.py", // default weasel
		"~":     "weasel",      // command type
	}
	// filter criteria operators...
	//  = equals		! not equals
	//  < less/before	> greater/after
	//  [ prefix		] suffix
	//  ~ regex match	^ regex non-match
	fltC = regexp.MustCompile(`^\s*(?P<col>[\w#$%(+"'-][ \w#$%&()+:;"',.?/-]*?)(?:\s*@(?P<attr>length|len|samples|samp|last|recent|avg|average|mean|mu|med|median|min|minimum|max|maximum|sdev|stdev|stddev|sigma)\b)?\s*(?P<op>[=!<>[\]~^])(?P<opd>.*)$`)
)

func ec2awsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if inst := m.data[1].(*ec2Detail).Inst[id]; inst != nil {
		s = fmt.Sprintf("%v", *inst)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func ebsawsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if vol := m.data[1].(*ebsDetail).Vol[id]; vol != nil {
		s = fmt.Sprintf("%v", *vol)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func rdsawsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if db := m.data[1].(*rdsDetail).DB[id]; db != nil {
		s = fmt.Sprintf("%v", *db)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func (sum hsU) series(typ byte, cur int32, span, recent int, truncate interface{}) (ser map[string][]float64) {
	ser = make(map[string][]float64)
	var s []float64
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			if s = ser[n]; s == nil {
				s = make([]float64, 0, span)
			}
			switch s = s[:h+1]; typ {
			case 'n':
				s[h] = float64(i.Usage) / 3600
			default:
				s[h] = i.Cost
			}
			ser[n] = s
		}
	}
	switch tr := truncate.(type) {
	case float64:
		tr = math.Abs(tr)
		for n, rct := range ser {
			avg := 0.0
			for _, v := range rct {
				avg += v
			}
			if avg /= float64(len(rct)); avg > -tr && tr > avg {
				delete(ser, n)
			}
		}
	case func([]float64) bool:
		for n, rct := range ser {
			if tr(rct) {
				delete(ser, n)
			}
		}
	}
	if len(ser) > 0 {
		for h := recent; h < span; h++ {
			for n, i := range sum[cur-int32(h)] {
				if s = ser[n]; s != nil {
					switch s = s[:h+1]; typ {
					case 'n':
						s[h] = float64(i.Usage) / 3600
					default:
						s[h] = i.Cost
					}
					ser[n] = s
				}
			}
		}
	}
	return
}

func (sum hsA) series(typ byte, cur int32, span, recent int, truncate interface{}) (ser map[string][]float64) {
	ser = make(map[string][]float64)
	var s []float64
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			if s = ser[n]; s == nil {
				s = make([]float64, 0, span)
			}
			s = s[:h+1]
			s[h], ser[n] = i, s
		}
	}
	switch tr := truncate.(type) {
	case float64:
		tr = math.Abs(tr)
		for n, rct := range ser {
			avg := 0.0
			for _, v := range rct {
				avg += v
			}
			if avg /= float64(len(rct)); avg > -tr && tr > avg {
				delete(ser, n)
			}
		}
	case func([]float64) bool:
		for n, rct := range ser {
			if tr(rct) {
				delete(ser, n)
			}
		}
	}
	if len(ser) > 0 {
		for h := recent; h < span; h++ {
			for n, i := range sum[cur-int32(h)] {
				if s = ser[n]; s != nil {
					s = s[:h+1]
					s[h], ser[n] = i, s
				}
			}
		}
	}
	return
}

func (sum hsC) series(typ byte, cur int32, span, recent int, truncate interface{}) (ser map[string][]float64) {
	ser = make(map[string][]float64)
	var s []float64
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			if s = ser[n]; s == nil {
				s = make([]float64, 0, span)
			}
			switch s = s[:h+1]; typ {
			case 'm':
				s[h] = i.Marg
			case 'p':
				if i.Bill != 0 {
					s[h] = i.Marg / i.Bill
				} else if i.Marg != 0 {
					s[h] = math.Copysign(maxPctMargin, i.Marg)
				}
			case 'c':
				s[h] = i.Bill - i.Marg
			case 'n':
				s[h] = float64(i.Calls)
			case 'd':
				s[h] = float64(i.Dur) / 600
			default:
				s[h] = i.Bill
			}
			ser[n] = s
		}
	}
	switch tr := truncate.(type) {
	case float64:
		tr = math.Abs(tr)
		for n, rct := range ser {
			avg := 0.0
			for _, v := range rct {
				avg += v
			}
			if avg /= float64(len(rct)); avg > -tr && tr > avg {
				delete(ser, n)
			}
		}
	case func([]float64) bool:
		for n, rct := range ser {
			if tr(rct) {
				delete(ser, n)
			}
		}
	}
	if len(ser) > 0 {
		for h := recent; h < span; h++ {
			for n, i := range sum[cur-int32(h)] {
				if s = ser[n]; s != nil {
					switch s = s[:h+1]; typ {
					case 'm':
						s[h] = i.Marg
					case 'p':
						if i.Bill != 0 {
							s[h] = i.Marg / i.Bill
						} else if i.Marg != 0 {
							s[h] = math.Copysign(maxPctMargin, i.Marg)
						}
					case 'c':
						s[h] = i.Bill - i.Marg
					case 'n':
						s[h] = float64(i.Calls)
					case 'd':
						s[h] = float64(i.Dur) / 600
					default:
						s[h] = i.Bill
					}
					ser[n] = s
				}
			}
		}
	}
	return
}

func (sum hnC) series(typ byte, cur int32, span, recent int, truncate interface{}) (ser map[string][]float64) {
	nser := make(map[tel.E164digest][]float64, 65535)
	var s []float64
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			if s = nser[n]; s == nil {
				s = make([]float64, 0, span)
			}
			switch s = s[:h+1]; typ {
			case 'm':
				s[h] = i.Marg
			case 'p':
				if i.Bill != 0 {
					s[h] = i.Marg / i.Bill
				} else if i.Marg != 0 {
					s[h] = math.Copysign(maxPctMargin, i.Marg)
				}
			case 'c':
				s[h] = i.Bill - i.Marg
			case 'n':
				s[h] = float64(i.Calls)
			case 'd':
				s[h] = float64(i.Dur) / 600
			default:
				s[h] = i.Bill
			}
			nser[n] = s
		}
	}
	switch tr := truncate.(type) {
	case float64:
		tr = math.Abs(tr)
		for n, rct := range nser {
			avg := 0.0
			for _, v := range rct {
				avg += v
			}
			if avg /= float64(len(rct)); avg > -tr && tr > avg {
				delete(nser, n)
			}
		}
	case func([]float64) bool:
		for n, rct := range nser {
			if tr(rct) {
				delete(nser, n)
			}
		}
	}
	if ser = make(map[string][]float64, len(nser)); len(nser) > 0 {
		for h := recent; h < span; h++ {
			for n, i := range sum[cur-int32(h)] {
				if s = nser[n]; s != nil {
					switch s = s[:h+1]; typ {
					case 'm':
						s[h] = i.Marg
					case 'p':
						if i.Bill != 0 {
							s[h] = i.Marg / i.Bill
						} else if i.Marg != 0 {
							s[h] = math.Copysign(maxPctMargin, i.Marg)
						}
					case 'c':
						s[h] = i.Bill - i.Marg
					case 'n':
						s[h] = float64(i.Calls)
					case 'd':
						s[h] = float64(i.Dur) / 600
					default:
						s[h] = i.Bill
					}
					nser[n] = s
				}
			}
		}
		for n, s := range nser {
			ser[n.String()] = s
		}
	}
	return
}

func seriesExtract(metric string, span, recent int, truncate interface{}) (res chan *cmon.SeriesRet, err error) {
	var acc *modAcc
	var sum interface{}
	var cur int32
	var typ byte
	if recent > span {
		span = recent
	}
	if recent <= 0 || span > 24*100 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(strings.SplitN(metric, "/", 2)[0], ".", 3)[:2], ".")].newAcc(); acc == nil {
		return nil, fmt.Errorf("model not found")
	} else if metric[len(metric)-2] == '/' {
		typ = metric[len(metric)-1]
	}

	switch metric {
	case "ec2.aws/acct", "ec2.aws/acct/n":
		sum, cur = acc.m.data[0].(*ec2Sum).ByAcct, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/region", "ec2.aws/region/n":
		sum, cur = acc.m.data[0].(*ec2Sum).ByRegion, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/sku", "ec2.aws/sku/n":
		sum, cur = acc.m.data[0].(*ec2Sum).BySKU, acc.m.data[0].(*ec2Sum).Current

	case "ebs.aws/acct", "ebs.aws/acct/n":
		sum, cur = acc.m.data[0].(*ebsSum).ByAcct, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/region", "ebs.aws/region/n":
		sum, cur = acc.m.data[0].(*ebsSum).ByRegion, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/sku", "ebs.aws/sku/n":
		sum, cur = acc.m.data[0].(*ebsSum).BySKU, acc.m.data[0].(*ebsSum).Current

	case "rds.aws/acct", "rds.aws/acct/n":
		sum, cur = acc.m.data[0].(*rdsSum).ByAcct, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/region", "rds.aws/region/n":
		sum, cur = acc.m.data[0].(*rdsSum).ByRegion, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/sku", "rds.aws/sku/n":
		sum, cur = acc.m.data[0].(*rdsSum).BySKU, acc.m.data[0].(*rdsSum).Current

	case "snap.aws/acct", "snap.aws/acct/n":
		sum, cur = acc.m.data[0].(*snapSum).ByAcct, acc.m.data[0].(*snapSum).Current
	case "snap.aws/region", "snap.aws/region/n":
		sum, cur = acc.m.data[0].(*snapSum).ByRegion, acc.m.data[0].(*snapSum).Current

	case "cur.aws/acct":
		sum, cur = acc.m.data[0].(*curSum).ByAcct, acc.m.data[0].(*curSum).Current
	case "cur.aws/region":
		sum, cur = acc.m.data[0].(*curSum).ByRegion, acc.m.data[0].(*curSum).Current
	case "cur.aws/typ":
		sum, cur = acc.m.data[0].(*curSum).ByTyp, acc.m.data[0].(*curSum).Current
	case "cur.aws/svc":
		sum, cur = acc.m.data[0].(*curSum).BySvc, acc.m.data[0].(*curSum).Current

	case "cdr.asp/term/cust", "cdr.asp/term/cust/m", "cdr.asp/term/cust/p", "cdr.asp/term/cust/c", "cdr.asp/term/cust/n", "cdr.asp/term/cust/d":
		sum, cur = acc.m.data[0].(*termSum).ByCust, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/geo", "cdr.asp/term/geo/m", "cdr.asp/term/geo/p", "cdr.asp/term/geo/c", "cdr.asp/term/geo/n", "cdr.asp/term/geo/d":
		sum, cur = acc.m.data[0].(*termSum).ByGeo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/sp", "cdr.asp/term/sp/m", "cdr.asp/term/sp/p", "cdr.asp/term/sp/c", "cdr.asp/term/sp/n", "cdr.asp/term/sp/d":
		sum, cur = acc.m.data[0].(*termSum).BySP, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/loc", "cdr.asp/term/loc/m", "cdr.asp/term/loc/p", "cdr.asp/term/loc/c", "cdr.asp/term/loc/n", "cdr.asp/term/loc/d":
		sum, cur = acc.m.data[0].(*termSum).ByLoc, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/to", "cdr.asp/term/to/m", "cdr.asp/term/to/p", "cdr.asp/term/to/c", "cdr.asp/term/to/n", "cdr.asp/term/to/d":
		sum, cur = acc.m.data[0].(*termSum).ByTo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/from", "cdr.asp/term/from/m", "cdr.asp/term/from/p", "cdr.asp/term/from/c", "cdr.asp/term/from/n", "cdr.asp/term/from/d":
		sum, cur = acc.m.data[0].(*termSum).ByFrom, acc.m.data[0].(*termSum).Current

	case "cdr.asp/orig/cust", "cdr.asp/orig/cust/m", "cdr.asp/orig/cust/p", "cdr.asp/orig/cust/c", "cdr.asp/orig/cust/n", "cdr.asp/orig/cust/d":
		sum, cur = acc.m.data[1].(*origSum).ByCust, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/geo", "cdr.asp/orig/geo/m", "cdr.asp/orig/geo/p", "cdr.asp/orig/geo/c", "cdr.asp/orig/geo/n", "cdr.asp/orig/geo/d":
		sum, cur = acc.m.data[1].(*origSum).ByGeo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/sp", "cdr.asp/orig/sp/m", "cdr.asp/orig/sp/p", "cdr.asp/orig/sp/c", "cdr.asp/orig/sp/n", "cdr.asp/orig/sp/d":
		sum, cur = acc.m.data[1].(*origSum).BySP, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/loc", "cdr.asp/orig/loc/m", "cdr.asp/orig/loc/p", "cdr.asp/orig/loc/c", "cdr.asp/orig/loc/n", "cdr.asp/orig/loc/d":
		sum, cur = acc.m.data[1].(*origSum).ByLoc, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/to", "cdr.asp/orig/to/m", "cdr.asp/orig/to/p", "cdr.asp/orig/to/c", "cdr.asp/orig/to/n", "cdr.asp/orig/to/d":
		sum, cur = acc.m.data[1].(*origSum).ByTo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/from", "cdr.asp/orig/from/m", "cdr.asp/orig/from/p", "cdr.asp/orig/from/c", "cdr.asp/orig/from/n", "cdr.asp/orig/from/d":
		sum, cur = acc.m.data[1].(*origSum).ByFrom, acc.m.data[1].(*origSum).Current

	default:
		return nil, fmt.Errorf("unknown metric")
	}
	res = make(chan *cmon.SeriesRet, 1)

	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer recover()
				close(res)
			}
		}()
		var ser map[string][]float64
		acc.reqR() // TODO: may relocate prior to acc.m.data reference
		switch sum := sum.(type) {
		case hsU:
			ser = sum.series(typ, cur, span, recent, truncate)
		case hsA:
			ser = sum.series(typ, cur, span, recent, truncate)
		case hsC:
			ser = sum.series(typ, cur, span, recent, truncate)
		case hnC:
			ser = sum.series(typ, cur, span, recent, truncate)
		}
		acc.rel()
		res <- &cmon.SeriesRet{From: cur, Series: ser}
		close(res)
	}()
	return
}

// globalTags builds a global map in part to flesh out EC2/EBS/snapshot tags by leveraging their transitive relationships
func globalTags(snaps, dbs int) (tags map[string]*cmon.TagMap) {
	tags = make(map[string]*cmon.TagMap, 32768)

	if ec2 := mMod["ec2.aws"].newAcc(); ec2 != nil && len(ec2.m.data) > 1 {
		func() {
			ec2.reqR()
			defer ec2.rel()
			for id, inst := range ec2.m.data[1].(*ec2Detail).Inst {
				t := cmon.TagMap{}.UpdateP(inst.Tag, "cmon:").UpdateN(settings, inst.Acct, "names", inst.Tag["cmon:Name"])
				tags[id] = &t
			}
		}()
	}
	// TODO: evaluate adding bypass for full TagMaps
	if ebs := mMod["ebs.aws"].newAcc(); ebs != nil && len(ebs.m.data) > 1 {
		func() {
			ebs.reqR()
			defer ebs.rel()
			for id, vol := range ebs.m.data[1].(*ebsDetail).Vol {
				var t *cmon.TagMap
				if strings.HasPrefix(vol.Mount, "i-") {
					t = tags[strings.SplitN(vol.Mount, ":", 2)[0]]
				}
				if t == nil {
					t = &cmon.TagMap{}
				}
				t.UpdateP(vol.Tag, "cmon:").UpdateN(settings, vol.Acct, "names", vol.Tag["cmon:Name"])
				tags[id] = t
			}
		}()
	}
	if snap := mMod["snap.aws"].newAcc(); snap != nil && len(snap.m.data) > 1 {
		func() {
			snap.reqR()
			defer snap.rel()
			for id, sd := range snap.m.data[1].(*snapDetail).Snap {
				var t *cmon.TagMap
				if sd.Vol != "" {
					t = tags[sd.Vol]
				}
				if t == nil {
					if snaps == 0 {
						continue
					}
					t = &cmon.TagMap{}
				}
				switch t.UpdateP(sd.Tag, "cmon:").UpdateN(settings, sd.Acct, "names", sd.Tag["cmon:Name"]); snaps {
				case 1:
					tags[id] = t
				case 2:
					tags[id], tags["snapshot/"+id] = t, t
				}
			}
		}()
	}
	if rds := mMod["rds.aws"].newAcc(); dbs > 0 && rds != nil && len(rds.m.data) > 1 {
		func() {
			rds.reqR()
			defer rds.rel()
			for id, db := range rds.m.data[1].(*rdsDetail).DB {
				t := cmon.TagMap{}.UpdateP(db.Tag, "cmon:")
				if t["cmon:Name"] == "" {
					if i := strings.LastIndexByte(id, ':') + 1; i > 0 {
						t.UpdateT("cmon:Name", id[i:])
					}
				}
				t.UpdateN(settings, db.Acct, "names", t["cmon:Name"])
				tags[id] = &t
			}
		}()
	}
	return
}

func tsattr(ts []float32, a string, minl, maxl float32) float32 {
	if len(ts) > 0 {
		switch a {
		case "length", "len", "samples", "samp":
			return float32(len(ts))
		case "last", "recent":
			return ts[len(ts)-1]
		case "median", "med":
			ss := make([]float32, len(ts))
			copy(ss, ts)
			sort.Slice(ss, func(i, j int) bool { return ss[i] < ss[j] })
			if m := len(ss) / 2; m*2 < len(ss) {
				return ss[m]
			} else {
				return (ss[m] + ss[m-1]) / 2
			}
		case "minimum", "min":
			min := maxl
			for _, v := range ts {
				if v <= minl {
					return minl
				} else if v < min {
					min = v
				}
			}
			return min
		case "maximum", "max":
			max := minl
			for _, v := range ts {
				if v >= maxl {
					return maxl
				} else if v > max {
					max = v
				}
			}
			return max
		case "stddev", "stdev", "sdev", "sigma":
			var mean, d, sdev float32
			for _, v := range ts {
				mean += v
			}
			mean /= float32(len(ts))
			for _, v := range ts {
				d = v - mean
				sdev += d * d
			}
			return float32(math.Sqrt(float64(sdev) / float64(len(ts))))
		case "avg", "average", "mean", "mu", "":
			var sum float32
			for _, v := range ts {
				sum += v
			}
			return sum / float32(len(ts))
		}
	}
	return 0
}

func active(since, last int, ap []int) float32 {
	var a int
	for i := 0; i+1 < len(ap); i += 2 {
		a += ap[i+1] - ap[i] + 1
	}
	return float32(a) / float32(last-since+1)
}

func atos(ts string) (s int64) {
	if s, _ = strconv.ParseInt(ts, 0, 0); s > 1e4 {
		if s < 3e6 { // hours/seconds in Unix epoch cutoff
			s *= 3600
		}
		return
	}
	ts = fmt.Sprintf("%-4.19s", ts)
	t, _ := time.Parse(time.RFC3339, ts+"-01-01T00:00:00Z"[len(ts)-4:])
	return t.Unix()
}

func skip(flt []func(...interface{}) bool, v ...interface{}) bool {
	for _, f := range flt {
		if !f(v...) {
			return true
		}
	}
	return false
}

func tstos(ts []float32) string {
	if len(ts) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%g", ts[0])
	for _, v := range ts[1:] {
		fmt.Fprintf(&b, " %g", v)
	}
	return b.String()
}

func (d *ec2Detail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	flt, adj := make([]func(...interface{}) bool, 0, 32), 3*fetchCycle
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Acct", "account", "acct":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*ec2Item).Acct
					return strings.HasPrefix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "]":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*ec2Item).Acct
					return strings.HasSuffix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*ec2Item).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) != ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*ec2Item).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) == ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Type", "type", "typ":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Typ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Typ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ec2Item).Typ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ec2Item).Typ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).Typ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).Typ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Plat", "platform", "plat":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Plat == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Plat != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ec2Item).Plat, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ec2Item).Plat, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).Plat) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).Plat) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Vol", "volume", "vol":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Vol == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Vol != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Vol < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Vol > n })
				}
			}
		case "AZ", "az":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AZ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AZ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ec2Item).AZ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ec2Item).AZ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).AZ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ec2Item).AZ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "VPC", "vpc":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).VPC == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).VPC != opd })
			}
		case "AMI", "ami":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AMI == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AMI != opd })
			}
		case "Spot", "spot":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Spot == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Spot != opd })
			}
		case "cmon:Name", "cmon:Env", "cmon:Prod", "cmon:Role", "cmon:Ver", "cmon:Prov", "cmon:Oper", "cmon:Bill", "cmon:Cust":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[1].(cmon.TagMap)[col], opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[1].(cmon.TagMap)[col], opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "CPU%", "CPU", "cpu%", "cpu":
			if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch col = "cpu"; op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ec2Item).Metric[col], attr, 0, 100) == float32(f) })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ec2Item).Metric[col], attr, 0, 100) != float32(f) })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ec2Item).Metric[col], attr, 0, 100) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ec2Item).Metric[col], attr, 0, 100) > float32(f) })
				}
			}
		case "State", "state", "stat", "st":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).State != opd })
			}
		case "Since", "since":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if s := atos(opd); s <= 0 {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Since > int(s) })
				}
			}
		case "Active%", "active%", "act%":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*ec2Item).Since, v[0].(*ec2Item).Last, v[0].(*ec2Item).Active) < float32(f)
					})
				case ">":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*ec2Item).Since, v[0].(*ec2Item).Last, v[0].(*ec2Item).Active) > float32(f)
					})
				}
			}
		case "Active", "active", "act":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch s, now := int(f*3600+0.5), int(time.Now().Unix()); op {
				case "=": // active opd*3600 seconds ago
					if flt = append(flt, func(v ...interface{}) bool {
						ap := v[0].(*ec2Item).Active
						for i, t := len(ap)-2, now-s; i >= 0; i -= 2 {
							if t < ap[i] {
							} else {
								return t <= ap[i+1]
							}
						}
						return false
					}); s > adj {
						adj = s
					}
				case "!": // not active opd*3600 seconds ago
					s = now - s
					flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*ec2Item).Active; len(ap) > 1 {
							for i := len(ap) - 1; i > 0; i -= 2 {
								if s <= ap[i] {
								} else {
									return i+1 == len(ap) || s < ap[i+1]
								}
							}
							return s < ap[0]
						}
						return true
					})
					adj = d.Current
				case "<": // last active to a point within opd hours
					if flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*ec2Item).Active; len(ap) > 1 {
							return now-ap[len(ap)-2] > s && now-ap[len(ap)-1] < s && ap[len(ap)-1] < d.Current
						}
						return false
					}); s > adj {
						adj = s
					}
				case ">": // last active since a point within opd hours
					flt = append(flt, func(v ...interface{}) bool {
						if ap, last := v[0].(*ec2Item).Active, v[0].(*ec2Item).Last; len(ap) > 1 && last >= d.Current {
							return now-ap[len(ap)-2] < s && ap[len(ap)-1] == last
						}
						return false
					})
				}
			}
		case "Rate", "rate":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Rate > float32(f) })
				}
			}
		case "ORate", "orate":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).ORate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).ORate > float32(f) })
				}
			}
		default:
			return 0, nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt) {
			return 0, nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return d.Current - adj, flt, nil
}

func (d *ec2Detail) table(acc *modAcc, res chan []string, rows, cur int, flt []func(...interface{}) bool) {
	tags, pg := globalTags(0, 0), smPage
	acc.reqR()
	defer acc.rel()
	for id, inst := range d.Inst {
		if inst.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.UpdateR(tags[id])
		if tag.UpdateP(settings.AWS.Accounts[inst.Acct], "cmon:"); inst.AZ != "" {
			tag.UpdateP(settings.AWS.Regions[inst.AZ[:len(inst.AZ)-1]], "cmon:")
		}
		if skip(flt, inst, tag.UpdateV(settings, inst.Acct)) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			inst.Acct + " " + settings.AWS.Accounts[inst.Acct]["~name"],
			inst.Typ,
			inst.Plat,
			strconv.FormatInt(int64(inst.Vol), 10),
			inst.AZ,
			inst.VPC,
			inst.AMI,
			inst.Spot,
			tag["cmon:Name"],
			tag["cmon:Env"],
			tag["cmon:Prod"],
			tag["cmon:Role"],
			tag["cmon:Ver"],
			tag["cmon:Prov"],
			tag["cmon:Oper"],
			tag["cmon:Bill"],
			tag["cmon:Cust"],
			tstos(inst.Metric["cpu"]),
			inst.State,
			time.Unix(int64(inst.Since), 0).UTC().Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(inst.Since, inst.Last, inst.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(inst.ORate), 'g', -1, 32),
			strconv.FormatFloat(float64(inst.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- row:
				continue
			default:
			}
		}
		acc.rel()
		res <- row
		pg = smPage
		acc.reqR()
	}
}

func (d *ebsDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	flt, adj := make([]func(...interface{}) bool, 0, 32), 3*fetchCycle
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Acct", "account", "acct":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*ebsItem).Acct
					return strings.HasPrefix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "]":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*ebsItem).Acct
					return strings.HasSuffix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*ebsItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) != ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*ebsItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) == ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Type", "type", "typ":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Typ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Typ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ebsItem).Typ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ebsItem).Typ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).Typ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).Typ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "GiB", "GB", "Size", "gib", "gb", "size", "siz":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).GiB == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).GiB != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).GiB < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).GiB > n })
				}
			}
		case "IOPS", "iops":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).IOPS == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).IOPS != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).IOPS < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).IOPS > n })
				}
			}
		case "MiBps", "MBps", "mibps", "mbps", "throughput", "thru":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).MiBps == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).MiBps != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).MiBps < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).MiBps > n })
				}
			}
		case "AZ", "az":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).AZ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).AZ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ebsItem).AZ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ebsItem).AZ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).AZ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).AZ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Mount", "mount", "mnt":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*ebsItem).Mount, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*ebsItem).Mount, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).Mount) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*ebsItem).Mount) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "cmon:Name", "cmon:Env", "cmon:Prod", "cmon:Role", "cmon:Ver", "cmon:Prov", "cmon:Oper", "cmon:Bill", "cmon:Cust":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[1].(cmon.TagMap)[col], opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[1].(cmon.TagMap)[col], opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "IO%", "IO", "io%", "io":
			if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch col = "io"; op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 100) == float32(f) })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 100) != float32(f) })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 100) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 100) > float32(f) })
				}
			}
		case "IOQ", "ioq":
			if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch col = strings.ToLower(col); op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 1e9) == float32(f) })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 1e9) != float32(f) })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 1e9) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*ebsItem).Metric[col], attr, 0, 1e9) > float32(f) })
				}
			}
		case "State", "state", "stat", "st":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).State != opd })
			}
		case "Since", "since":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if s := atos(opd); s <= 0 {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Since > int(s) })
				}
			}
		case "Active%", "active%", "act%":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*ebsItem).Since, v[0].(*ebsItem).Last, v[0].(*ebsItem).Active) < float32(f)
					})
				case ">":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*ebsItem).Since, v[0].(*ebsItem).Last, v[0].(*ebsItem).Active) > float32(f)
					})
				}
			}
		case "Active", "active", "act":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch s, now := int(f*3600+0.5), int(time.Now().Unix()); op {
				case "=": // active opd*3600 seconds ago
					if flt = append(flt, func(v ...interface{}) bool {
						ap := v[0].(*ebsItem).Active
						for i, t := len(ap)-2, now-s; i >= 0; i -= 2 {
							if t < ap[i] {
							} else {
								return t <= ap[i+1]
							}
						}
						return false
					}); s > adj {
						adj = s
					}
				case "!": // not active opd*3600 seconds ago
					s = now - s
					flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*ebsItem).Active; len(ap) > 1 {
							for i := len(ap) - 1; i > 0; i -= 2 {
								if s <= ap[i] {
								} else {
									return i+1 == len(ap) || s < ap[i+1]
								}
							}
							return s < ap[0]
						}
						return true
					})
					adj = d.Current
				case "<": // last active to a point within opd hours
					if flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*ebsItem).Active; len(ap) > 1 {
							return now-ap[len(ap)-2] > s && now-ap[len(ap)-1] < s && ap[len(ap)-1] < d.Current
						}
						return false
					}); s > adj {
						adj = s
					}
				case ">": // last active since a point within opd hours
					flt = append(flt, func(v ...interface{}) bool {
						if ap, last := v[0].(*ebsItem).Active, v[0].(*ebsItem).Last; len(ap) > 1 && last >= d.Current {
							return now-ap[len(ap)-2] < s && ap[len(ap)-1] == last
						}
						return false
					})
				}
			}
		case "Rate", "rate":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Rate > float32(f) })
				}
			}
		default:
			return 0, nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt) {
			return 0, nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return d.Current - adj, flt, nil
}

func (d *ebsDetail) table(acc *modAcc, res chan []string, rows, cur int, flt []func(...interface{}) bool) {
	tags, pg := globalTags(0, 0), smPage
	acc.reqR()
	defer acc.rel()
	for id, vol := range d.Vol {
		if vol.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.UpdateR(tags[id])
		if tag.UpdateP(settings.AWS.Accounts[vol.Acct], "cmon:"); vol.AZ != "" {
			tag.UpdateP(settings.AWS.Regions[vol.AZ[:len(vol.AZ)-1]], "cmon:")
		}
		if skip(flt, vol, tag.UpdateV(settings, vol.Acct)) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			vol.Acct + " " + settings.AWS.Accounts[vol.Acct]["~name"],
			vol.Typ,
			strconv.FormatInt(int64(vol.GiB), 10),
			strconv.FormatInt(int64(vol.IOPS), 10),
			strconv.FormatInt(int64(vol.MiBps), 10),
			vol.AZ,
			vol.Mount,
			tag["cmon:Name"],
			tag["cmon:Env"],
			tag["cmon:Prod"],
			tag["cmon:Role"],
			tag["cmon:Ver"],
			tag["cmon:Prov"],
			tag["cmon:Oper"],
			tag["cmon:Bill"],
			tag["cmon:Cust"],
			tstos(vol.Metric["io"]),
			tstos(vol.Metric["ioq"]),
			vol.State,
			time.Unix(int64(vol.Since), 0).UTC().Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(vol.Since, vol.Last, vol.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(vol.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- row:
				continue
			default:
			}
		}
		acc.rel()
		res <- row
		pg = smPage
		acc.reqR()
	}
}

func (d *rdsDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	flt, adj := make([]func(...interface{}) bool, 0, 32), 3*fetchCycle
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Acct", "account", "acct":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*rdsItem).Acct
					return strings.HasPrefix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "]":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*rdsItem).Acct
					return strings.HasSuffix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*rdsItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) != ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*rdsItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) == ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Type", "type", "typ":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Typ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Typ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*rdsItem).Typ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*rdsItem).Typ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Typ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Typ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "SType", "stype", "styp":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).STyp == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).STyp != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*rdsItem).STyp, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*rdsItem).STyp, opd) })
			}
		case "GiB", "GB", "Size", "gib", "gb", "size", "siz":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).GiB == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).GiB != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).GiB < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).GiB > n })
				}
			}
		case "IOPS", "iops":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).IOPS == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).IOPS != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).IOPS < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).IOPS > n })
				}
			}
		case "Engine", "engine", "eng":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Engine == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Engine != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*rdsItem).Engine, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*rdsItem).Engine, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Engine) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Engine) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "EngVer", "engver", "engv":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Ver == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Ver != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*rdsItem).Ver, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*rdsItem).Ver, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Ver) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Ver) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Lic", "license", "lic":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Lic == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Lic != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*rdsItem).Lic, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*rdsItem).Lic, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Lic) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*rdsItem).Lic) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "AZ", "az":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(string) == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(string) != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[1].(string), opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[1].(string), opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(string)) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(string)) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "VPC", "vpc":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).VPC == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).VPC != opd })
			}
		case "cmon:Name", "cmon:Env", "cmon:Prod", "cmon:Role", "cmon:Ver", "cmon:Prov", "cmon:Oper", "cmon:Bill", "cmon:Cust":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[2].(cmon.TagMap)[col] == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[2].(cmon.TagMap)[col] != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[2].(cmon.TagMap)[col], opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[2].(cmon.TagMap)[col], opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[2].(cmon.TagMap)[col]) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[2].(cmon.TagMap)[col]) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "CPU%", "CPU", "cpu%", "cpu":
			if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch col = "cpu"; op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 100) == float32(f) })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 100) != float32(f) })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 100) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 100) > float32(f) })
				}
			}
		case "IOQ", "ioq", "Conn", "conn", "Mem", "mem", "Sto", "sto":
			if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch col = strings.ToLower(col); op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 1e9) == float32(f) })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 1e9) != float32(f) })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 1e9) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return tsattr(v[0].(*rdsItem).Metric[col], attr, 0, 1e9) > float32(f) })
				}
			}
		case "State", "state", "stat", "st":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).State != opd })
			}
		case "Since", "since":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if s := atos(opd); s <= 0 {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Since > int(s) })
				}
			}
		case "Active%", "active%", "act%":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*rdsItem).Since, v[0].(*rdsItem).Last, v[0].(*rdsItem).Active) < float32(f)
					})
				case ">":
					flt = append(flt, func(v ...interface{}) bool {
						return active(v[0].(*rdsItem).Since, v[0].(*rdsItem).Last, v[0].(*rdsItem).Active) > float32(f)
					})
				}
			}
		case "Active", "active", "act":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch s, now := int(f*3600+0.5), int(time.Now().Unix()); op {
				case "=": // active opd*3600 seconds ago
					if flt = append(flt, func(v ...interface{}) bool {
						ap := v[0].(*rdsItem).Active
						for i, t := len(ap)-2, now-s; i >= 0; i -= 2 {
							if t < ap[i] {
							} else {
								return t <= ap[i+1]
							}
						}
						return false
					}); s > adj {
						adj = s
					}
				case "!": // not active opd*3600 seconds ago
					s = now - s
					flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*rdsItem).Active; len(ap) > 1 {
							for i := len(ap) - 1; i > 0; i -= 2 {
								if s <= ap[i] {
								} else {
									return i+1 == len(ap) || s < ap[i+1]
								}
							}
							return s < ap[0]
						}
						return true
					})
					adj = d.Current
				case "<": // last active to a point within opd hours
					if flt = append(flt, func(v ...interface{}) bool {
						if ap := v[0].(*rdsItem).Active; len(ap) > 1 {
							return now-ap[len(ap)-2] > s && now-ap[len(ap)-1] < s && ap[len(ap)-1] < d.Current
						}
						return false
					}); s > adj {
						adj = s
					}
				case ">": // last active since a point within opd hours
					flt = append(flt, func(v ...interface{}) bool {
						if ap, last := v[0].(*rdsItem).Active, v[0].(*rdsItem).Last; len(ap) > 1 && last >= d.Current {
							return now-ap[len(ap)-2] < s && ap[len(ap)-1] == last
						}
						return false
					})
				}
			}
		case "Rate", "rate":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Rate > float32(f) })
				}
			}
		default:
			return 0, nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt) {
			return 0, nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return d.Current - adj, flt, nil
}

func (d *rdsDetail) table(acc *modAcc, res chan []string, rows, cur int, flt []func(...interface{}) bool) {
	var az string
	pg := smPage
	acc.reqR()
	defer acc.rel()
	for id, db := range d.DB {
		if db.Last < cur {
			continue
		} else if az = db.AZ; db.MultiAZ {
			az += "+"
		}
		tag := cmon.TagMap{}.UpdateP(db.Tag, "cmon:")
		if tag["cmon:Name"] == "" {
			if i := strings.LastIndexByte(id, ':') + 1; i > 0 {
				tag.UpdateT("cmon:Name", id[i:])
			}
		}
		if tag.UpdateN(settings, db.Acct, "names", tag["cmon:Name"]).UpdateP(settings.AWS.Accounts[db.Acct], "cmon:"); db.AZ != "" {
			tag.UpdateP(settings.AWS.Regions[db.AZ[:len(db.AZ)-1]], "cmon:")
		}
		if skip(flt, db, az, tag.UpdateV(settings, db.Acct)) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			db.Acct + " " + settings.AWS.Accounts[db.Acct]["~name"],
			db.Typ,
			db.STyp,
			strconv.FormatInt(int64(db.GiB), 10),
			strconv.FormatInt(int64(db.IOPS), 10),
			db.Engine,
			db.Ver,
			db.Lic,
			az,
			db.VPC,
			tag["cmon:Name"],
			tag["cmon:Env"],
			tag["cmon:Prod"],
			tag["cmon:Role"],
			tag["cmon:Ver"],
			tag["cmon:Prov"],
			tag["cmon:Oper"],
			tag["cmon:Bill"],
			tag["cmon:Cust"],
			tstos(db.Metric["cpu"]),
			tstos(db.Metric["ioq"]),
			tstos(db.Metric["conn"]),
			tstos(db.Metric["mem"]),
			tstos(db.Metric["sto"]),
			db.State,
			time.Unix(int64(db.Since), 0).UTC().Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(db.Since, db.Last, db.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(db.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- row:
				continue
			default:
			}
		}
		acc.rel()
		res <- row
		pg = smPage
		acc.reqR()
	}
}

func (d *snapDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Acct", "account", "acct":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*snapItem).Acct
					return strings.HasPrefix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "]":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*snapItem).Acct
					return strings.HasSuffix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*snapItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) != ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*snapItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) == ""
					})
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Type", "type", "typ":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Typ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Typ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*snapItem).Typ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*snapItem).Typ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Typ) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Typ) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Size", "size", "siz":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Size < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Size > float32(f) })
				}
			}
		case "VSize", "vsize", "vsiz":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).VSiz == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).VSiz != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).VSiz < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).VSiz > n })
				}
			}
		case "Reg", "region", "reg":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Reg == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Reg != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*snapItem).Reg, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*snapItem).Reg, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Reg) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Reg) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Vol", "volume", "vol":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Vol == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Vol != opd })
			}
		case "Par", "parent", "par":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Par == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Par != opd })
			}
		case "Desc", "description", "desc":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Desc == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Desc != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*snapItem).Desc, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*snapItem).Desc, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Desc) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*snapItem).Desc) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "cmon:Name", "cmon:Env", "cmon:Prod", "cmon:Role", "cmon:Ver", "cmon:Prov", "cmon:Oper", "cmon:Bill", "cmon:Cust":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[1].(cmon.TagMap)[col], opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[1].(cmon.TagMap)[col], opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) != "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) == "" })
				} else {
					return 0, nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Since", "since":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if s := atos(opd); s <= 0 {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Since > int(s) })
				}
			}
		case "Rate", "rate":
			if attr != "" {
				return 0, nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Rate > float32(f) })
				}
			}
		default:
			return 0, nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt) {
			return 0, nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return d.Current - 3*fetchCycle, flt, nil
}

func (d *snapDetail) table(acc *modAcc, res chan []string, rows, cur int, flt []func(...interface{}) bool) {
	tags, pg := globalTags(1, 0), smPage
	acc.reqR()
	defer acc.rel()
	for id, snap := range d.Snap {
		if snap.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.UpdateR(tags[id]).UpdateP(settings.AWS.Accounts[snap.Acct], "cmon:").UpdateP(
			settings.AWS.Regions[snap.Reg], "cmon:")
		if skip(flt, snap, tag.UpdateV(settings, snap.Acct)) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			snap.Acct + " " + settings.AWS.Accounts[snap.Acct]["~name"],
			snap.Typ,
			strconv.FormatFloat(float64(snap.Size), 'g', -1, 32),
			strconv.FormatInt(int64(snap.VSiz), 10),
			snap.Reg,
			snap.Vol,
			snap.Par,
			snap.Desc,
			tag["cmon:Name"],
			tag["cmon:Env"],
			tag["cmon:Prod"],
			tag["cmon:Role"],
			tag["cmon:Ver"],
			tag["cmon:Prov"],
			tag["cmon:Oper"],
			tag["cmon:Bill"],
			tag["cmon:Cust"],
			time.Unix(int64(snap.Since), 0).UTC().Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(snap.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- row:
				continue
			default:
			}
		}
		acc.rel()
		res <- row
		pg = smPage
		acc.reqR()
	}
}

func (d *hiD) filters(criteria []string) ([]func(...interface{}) bool, error) {
	var ct []string
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Loc", "loc":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			var sl tel.SLmap
			switch sl.Load(nil); op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return sl.Name(v[0].(*cdrItem).Info>>locShift) == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return sl.Name(v[0].(*cdrItem).Info>>locShift) != opd })
			}
		case "To", "to":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).To.String() == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).To.String() != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*cdrItem).To.String(), opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*cdrItem).To.String(), opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).To.String()) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).To.String()) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "From", "from", "fr":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).From.String() == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).From.String() != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*cdrItem).From.String(), opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*cdrItem).From.String(), opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).From.String()) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).From.String()) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Prov", "prov", "sp":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			var sp tel.SPmap
			switch sp.Load(nil); op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return sp.Name(v[0].(*cdrItem).Info&spMask) == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return sp.Name(v[0].(*cdrItem).Info&spMask) != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(sp.Name(v[0].(*cdrItem).Info&spMask), opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(sp.Name(v[0].(*cdrItem).Info&spMask), opd) })
			}
		case "CustApp", "custapp", "cust":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Cust == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Cust != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*cdrItem).Cust, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*cdrItem).Cust, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).Cust) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*cdrItem).Cust) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Start", "start", "time":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if s := atos(opd); s <= 0 {
				return nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(int64)+int64(v[0].(*cdrItem).Time&offMask) < s })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(int64)+int64(v[0].(*cdrItem).Time&offMask) > s })
				}
			}
		case "Min", "min", "dur":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return float32(v[0].(*cdrItem).Time>>durShift)/600 < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return float32(v[0].(*cdrItem).Time>>durShift)/600 > float32(f) })
				}
			}
		case "Tries", "tries":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(*cdrItem).Info>>triesShift&triesMask) == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(*cdrItem).Info>>triesShift&triesMask) != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(*cdrItem).Info>>triesShift&triesMask) < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(*cdrItem).Info>>triesShift&triesMask) > n })
				}
			}
		case "Billable", "billable", "bill":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Bill < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Bill > float32(f) })
				}
			}
		case "Margin", "margin", "marg":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Marg < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Marg > float32(f) })
				}
			}
		default:
			return nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt) {
			return nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return flt, nil
}

func (d *hiD) table(acc *modAcc, res chan []string, rows int, flt []func(...interface{}) bool) {
	var sp tel.SPmap
	var sl tel.SLmap
	sp.Load(nil)
	sl.Load(nil)
	pg := smPage
	acc.reqR()
	defer acc.rel()
outerLoop:
	for h, hm := range *d {
		t := int64(h) * 3600
		for id, cdr := range hm {
			if skip(flt, cdr, t) {
				continue
			} else if rows--; rows == 0 {
				break outerLoop
			}

			row := []string{
				fmt.Sprintf("0x%016X", id&idMask),
				sl.Name(cdr.Info >> locShift),
				cdr.To.String(),
				cdr.From.String(),
				sp.Name(cdr.Info & spMask),
				cdr.Cust,
				time.Unix(t+int64(cdr.Time&offMask), 0).UTC().Format("2006-01-02 15:04:05"),
				strconv.FormatFloat(float64(cdr.Time>>durShift)/600, 'g', -1, 32),
				strconv.FormatInt(int64(cdr.Info>>triesShift&triesMask), 10),
				strconv.FormatFloat(float64(cdr.Bill), 'g', -1, 32),
				strconv.FormatFloat(float64(cdr.Marg), 'g', -1, 32),
			}
			if pg--; pg >= 0 {
				select {
				case res <- row:
					continue
				default:
				}
			}
			acc.rel()
			res <- row
			pg = smPage
			acc.reqR()
		}
	}
}

func tableExtract(n string, rows int, criteria []string) (res chan []string, err error) {
	var acc *modAcc
	var cur int
	var flt []func(...interface{}) bool
	if rows++; rows < 0 || rows == 1 || rows > maxTableRows+1 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(strings.SplitN(n, "/", 2)[0], ".", 3)[:2], ".")].newAcc(); acc == nil || len(acc.m.data) < 2 {
		return nil, fmt.Errorf("model not found")
	} else {
		switch err = fmt.Errorf("unsupported model"); det := acc.m.data[1].(type) {
		case *ec2Detail:
			cur, flt, err = det.filters(criteria)
		case *ebsDetail:
			cur, flt, err = det.filters(criteria)
		case *rdsDetail:
			cur, flt, err = det.filters(criteria)
		case *snapDetail:
			cur, flt, err = det.filters(criteria)
		case *origSum:
			switch n {
			case "cdr.asp/term":
				flt, err = acc.m.data[2].(*termDetail).CDR.filters(criteria)
			case "cdr.asp/orig":
				flt, err = acc.m.data[3].(*origDetail).CDR.filters(criteria)
			}
		}
		if err != nil {
			return
		}
	}

	res = make(chan []string, 32)
	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer recover()
				close(res)
			}
		}()

		switch det := acc.m.data[1].(type) {
		case *ec2Detail:
			det.table(acc, res, rows, cur, flt)
		case *ebsDetail:
			det.table(acc, res, rows, cur, flt)
		case *rdsDetail:
			det.table(acc, res, rows, cur, flt)
		case *snapDetail:
			det.table(acc, res, rows, cur, flt)
		case *origSum:
			switch n {
			case "cdr.asp/term":
				acc.m.data[2].(*termDetail).CDR.table(acc, res, rows, flt)
			case "cdr.asp/orig":
				acc.m.data[3].(*origDetail).CDR.table(acc, res, rows, flt)
			}
		}
		close(res)
	}()
	return
}

func (d *curDetail) filters(criteria []string) ([]func(...interface{}) bool, error) {
	var ct []string
	flt, xc := make([]func(...interface{}) bool, 0, 32), 0
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "AWS Account", "account", "acct":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "[":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*curItem).Acct
					return strings.HasPrefix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "]":
				flt = append(flt, func(v ...interface{}) bool {
					a := v[0].(*curItem).Acct
					return strings.HasSuffix(a+" "+settings.AWS.Accounts[a]["~name"], opd)
				})
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*curItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) != ""
					})
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool {
						a := v[0].(*curItem).Acct
						return re.FindString(a+" "+settings.AWS.Accounts[a]["~name"]) == ""
					})
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Type", "type", "typ":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Typ == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Typ != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).Typ, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).Typ, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Typ) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Typ) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Service", "service", "serv", "svc":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Svc == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Svc != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).Svc, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).Svc, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Svc) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Svc) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Usage Type", "utype", "utyp", "ut":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).UTyp == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).UTyp != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).UTyp, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).UTyp, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).UTyp) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).UTyp) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Operation", "operation", "oper", "op":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).UOp == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).UOp != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).UOp, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).UOp, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).UOp) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).UOp) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Region", "region", "reg":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Reg == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Reg != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).Reg, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).Reg, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Reg) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Reg) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Resource ID", "resource", "rid":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).RID == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).RID != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).RID, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).RID, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).RID) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).RID) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "Item Description", "description", "descr", "desc":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Desc == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Desc != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).Desc, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).Desc, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Desc) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Desc) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "cmon:Name", "cmon:Env", "cmon:Prod", "cmon:Role", "cmon:Ver", "cmon:Prov", "cmon:Oper", "cmon:Bill", "cmon:Cust":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			}
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[1].(cmon.TagMap)[col] != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[1].(cmon.TagMap)[col], opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[1].(cmon.TagMap)[col], opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[1].(cmon.TagMap)[col]) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "PU", "pu":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[2].(float32) < float32(f) })
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return math.Abs(float64(v[2].(float32)-float32(f))) < 0.0007 })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[2].(float32) > float32(f) })
				}
			}
		case "Recs", "recs", "rec", "Usage", "usage", "usg":
			xc++
		default:
			return nil, fmt.Errorf("unknown column %q in criteria %q", col, c)
		}
		if nc == len(flt)+xc {
			return nil, fmt.Errorf("%q operator not supported for %q column", op, col)
		}
	}
	return flt, nil
}

func (d *curDetail) rfilters(criteria []string) ([]func(...interface{}) bool, error) {
	var ct []string
	flt := make([]func(...interface{}) bool, 0, 32)
	for _, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 4 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, attr, op, opd := ct[1], ct[2], ct[3], ct[4]
		switch col {
		case "Recs", "recs", "rec":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if n, err := strconv.Atoi(opd); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			} else {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(int16)) == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(int16)) != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(int16)) < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return int(v[0].(int16)) > n })
				default:
					return nil, fmt.Errorf("%q operator not supported for %q column", op, col)
				}
			}
		case "Usage", "usage", "usg":
			if attr != "" {
				return nil, fmt.Errorf("%q attribute not supported for %q column", attr, col)
			} else if f, err := strconv.ParseFloat(opd, 32); err != nil {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			} else {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(float32) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(float32) > float32(f) })
				default:
					return nil, fmt.Errorf("%q operator not supported for %q column", op, col)
				}
			}
		}
	}
	return flt, nil
}

func (d *curDetail) table(li *curItem, from, to int32, un int16, tr float32, id string, tag cmon.TagMap, puf float32, dts string, flt []func(...interface{}) bool) func() []string {
	var husg func(int32) float32
	var rate float32
	if un < 720 {
		switch rate = li.Chg / li.Usg; {
		case len(li.HMap) > 0 && li.HMap[0]>>hrBMShift == hrBitmap:
			off := int32(li.Recs>>foffShift&foffMask) - 32 + hrBMShift
			u := li.Usg / float32(li.Recs>>recsShift+1)
			husg = func(h int32) float32 {
				if i := h - off; li.HMap[i>>5]>>(31-i&31)&1 == 0 {
					return 0
				}
				return u
			}
		case len(li.HMap) > 0:
			var hu [usgIndex + 1]float32
			for _, m := range li.HMap {
				r, b, u := m>>rangeShift, m&baseMask, float32(0)
				if r += b; b < uint32(from) {
					b = uint32(from)
				}
				if r > uint32(to) {
					r = uint32(to)
				}
				if b > r {
					continue
				} else if ur := m >> usgShift & usgMask; ur > usgIndex {
					u = float32(ur - usgIndex)
				} else {
					u = li.HUsg[ur]
				}
				for hu[b], b = u, b+1; b <= r; hu[b], b = u, b+1 {
				}
			}
			husg = func(h int32) float32 {
				return hu[h]
			}
		case len(li.HUsg) > 0:
			off := int32(li.Recs >> foffShift & foffMask)
			husg = func(h int32) float32 {
				return li.HUsg[h-off]
			}
		default:
			u := li.Usg / float32(li.Recs>>recsShift+1)
			husg = func(h int32) float32 {
				return u
			}
		}
	}
	id = dts[:8] + id
	acct, pu := li.Acct+" "+settings.AWS.Accounts[li.Acct]["~name"], strconv.FormatFloat(float64(puf), 'g', -1, 32)

	return func() []string {
		var rec int16
		var usg, chg float32
		if from > to {
			return nil
		}
		switch un {
		case 1: // hourly
			for rec = 1; ; {
				if usg = husg(from); usg != 0 {
					if chg = usg * rate; (chg > tr || -tr > chg) && !skip(flt, rec, usg) {
						dts = dts[:8] + fmt.Sprintf("%02d %02d:00", from/24+1, from%24)
						from++
						break
					}
				}
				if from++; from > to {
					return nil
				}
			}
		case 24: // daily
			for day := from - from%24 + 23; ; day, rec, usg = day+24, 0, 0 {
				if day > to {
					day = to
				}
				for ; from <= day; from++ {
					if u := husg(from); u != 0 {
						rec++
						usg += u
					}
				}
				if chg = usg * rate; (chg > tr || -tr > chg) && !skip(flt, rec, usg) {
					dts = dts[:8] + fmt.Sprintf("%02d", day/24+1)
					break
				} else if from > to {
					return nil
				}
			}
		default: // monthly
			if rec, usg, chg, from = int16(li.Recs>>recsShift+1), li.Usg, li.Chg, to+1; skip(flt, rec, usg) {
				return nil
			}
		}

		return []string{
			id,
			dts,
			acct,
			li.Typ,
			li.Svc,
			li.UTyp,
			li.UOp,
			li.Reg,
			li.RID,
			li.Desc,
			tag["cmon:Name"],
			tag["cmon:Env"],
			tag["cmon:Prod"],
			tag["cmon:Role"],
			tag["cmon:Ver"],
			tag["cmon:Prov"],
			tag["cmon:Oper"],
			tag["cmon:Bill"],
			tag["cmon:Cust"],
			strconv.FormatInt(int64(rec), 10),
			pu,
			strconv.FormatFloat(float64(usg), 'g', -1, 32),
			strconv.FormatFloat(float64(chg), 'g', -1, 32),
		}
	}
}

func curtabExtract(from, to int32, units int16, rows int, truncate float64, criteria []string) (res chan []string, err error) {
	var acc *modAcc
	var sum *curSum
	var cur *curDetail
	var flt, rflt []func(...interface{}) bool
	if rows++; from > to || units < 1 || rows < 0 || rows == 1 || rows > maxTableRows+1 || truncate < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod["cur.aws"].newAcc(); acc == nil {
		return nil, fmt.Errorf("\"cur.aws\" model not found")
	} else if sum, cur = acc.m.data[0].(*curSum), acc.m.data[1].(*curDetail); cur == nil {
	} else if flt, err = cur.filters(criteria); err != nil {
		return
	} else if rflt, err = cur.rfilters(criteria); err != nil {
		return
	}

	if res = make(chan []string, 32); from <= 0 {
		acc.reqRt(0)
		from += sum.Current
		to += sum.Current
		acc.rel()
	}
	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer recover()
				close(res)
			}
		}()
		pg, trunc, tags := lgPage, float32(truncate), globalTags(2, 1)
		acc.reqR()
	outerLoop:
		for mo, hrs := range cur.Month {
			if to >= hrs[0] && hrs[1] >= from {
				mfr, mto, dts := from, to, mo[:4]+"-"+mo[4:]+"-01" // +" "+hh+":00"
				if mfr -= hrs[0]; mfr < 0 {
					mfr = 0
				}
				if mto > hrs[1] {
					mto = hrs[1]
				}
				mto -= hrs[0]
				ifr, ito := mfr, mto
				for id, li := range cur.Line[mo] {
					if li.Chg <= trunc && -trunc <= li.Chg {
						continue
					} else if units < 720 {
						if ifr = int32(li.Recs >> foffShift & foffMask); mfr > ifr {
							ifr = mfr
						}
						if ito = int32(li.Recs&toffMask - 1); mto < ito {
							ito = mto
						}
						if ifr > ito {
							continue
						}
					}
					if pg--; pg < 0 {
						acc.rel()
						pg = lgPage
						acc.reqR()
					}
					if pu, tag := sum.Hist.ppuse(li.RID, from, to), (cmon.TagMap{}).UpdateR(tags[li.RID]).Update(cmon.TagMap{
						"cmon:Name": li.Name,
						"cmon:Env":  li.Env,
						"cmon:Prod": li.Prod,
						"cmon:Role": li.Role,
						"cmon:Ver":  li.Ver,
						"cmon:Prov": li.Prov,
						"cmon:Oper": li.Oper,
						"cmon:Bill": li.Bill,
						"cmon:Cust": li.Cust,
					}).UpdateN(settings, li.Acct, "names", li.Name).UpdateN(settings, li.Acct, "RIDs", li.RID).UpdateP(
						settings.AWS.Accounts[li.Acct], "cmon:").UpdateP(settings.AWS.Regions[li.Reg], "cmon:"); skip(flt, li,
						tag.UpdateV(settings, li.Acct), pu) {
						continue
					} else if item := cur.table(li, ifr, ito, units, trunc, id, tag, pu, dts, rflt); item != nil {
						for row := item(); row != nil; row = item() {
							if rows--; rows == 0 {
								break outerLoop
							}
							select {
							case res <- row:
							default:
								acc.rel()
								res <- row
								pg = lgPage
								acc.reqR()
							}
						}
					}
				}
			}
		}
		acc.rel()
		close(res)
	}()
	return
}

func ec2Rater() func(*varexEnv, string, string) float32 {
	var rates aws.Rater
	rates.Load(nil, "EC2")
	return func(e *varexEnv, typ, plat string) float32 {
		if r := rates.Lookup(&aws.RateKey{
			Region: e.reg,
			Typ:    typ,
			Plat:   plat,
			Terms:  settings.AWS.SavPlan,
		}); r != nil {
			return r.Rate * settings.AWS.UsageAdj
		}
		return 0
	}
}
func ebsRater() func(*varexEnv, string, float32, float32) float32 {
	var rates aws.EBSRater
	rates.Load(nil)
	return func(e *varexEnv, typ string, gib, iops float32) float32 {
		if r := rates.Lookup(&aws.EBSRateKey{
			Region: e.reg,
			Typ:    typ,
		}); r != nil {
			if typ == "gp3" {
				iops -= 3000
			}
			return (r.SZrate*gib + r.IOrate*iops) * settings.AWS.UsageAdj
		}
		return 0
	}
}
func adjIOPS(v *cmon.VarVolume, gib float32) (iops float32) {
	switch iops = v.IOPS; v.SType {
	case "sc1", "st1", "standard":
		iops = 0
	case "gp2":
		iops = gib * 3
		fallthrough
	case "io1", "io2":
		if iops < 100 {
			iops = 100
		}
	default:
		if iops < 3000 {
			iops = 3000
		}
	}
	return
}
func platFmt(plat, suffix string) string {
	switch plat {
	case "sqlserver-se":
		return "SQL/SE" + suffix
	case "sqlserver-ee":
		return "SQL/EE" + suffix
	case "windows":
		return "Windows" + suffix
	case "rhel":
		return "RHEL" + suffix
	case "":
		return "Linux" + suffix
	default:
		return plat + suffix
	}
}
func variance(rows int, scan map[string]*varexEnv, res chan []string) {
	ec2Rates, ebsRates := ec2Rater(), ebsRater()
	for eref, e := range scan {
		mmm, t := make(map[string][]int, 64), settings.Variance.Templates[e.tref]
		for rref, mm := range t.EC2 {
			mmm[rref] = mm
		}
		for rref, mm := range t.Envs[eref].EC2 {
			mmm[rref] = mm // make min/max map by environment with Envs overriding EC2 template settings
		}
		for rref, is := range e.ec2 {
			rs, mm := settings.Variance.EC2[rref], mmm[rref]
			switch {
			case rref == "~":
				rref = "(unknown)"
				fallthrough
			case mm[1] == 0:
				for _, i := range is {
					c, d := i.arate, func() string {
						if rs == nil {
							return fmt.Sprintf("%v %v with %v volumes", i.itype, platFmt(i.plat, " instance"), len(i.vols))
						}
						return fmt.Sprintf("%v %v %v with %v volumes", rs.Descr, i.itype, platFmt(i.plat, " instance"), len(i.vols))
					}
					for _, v := range i.vols {
						c += v.rate
					}
					res <- []string{ // emit unknown "perfect" excess variant
						i.id,
						i.name,
						fmt.Sprintf("EC2:%v", rref),
						d(),
						eref,
						e.reg,
						e.tref,
						"(excess)",
						"",
						"",
						strconv.FormatFloat(float64(c*730*12), 'g', -1, 32),
						strconv.FormatFloat(float64(-c*730*12), 'g', -1, 32),
					}
				}
				continue
			}
			for _, i := range is {
				vt, cv, c := "inst type ok", "0", float32(0)
				if i.itype != rs.IType {
					if vt, c = "inst type", ec2Rates(e, i.itype, i.plat); c > 0 {
						// calculate annual cost variance with IType, factoring in active %, Savings Plans & EDP discount
						cv = strconv.FormatFloat(float64((ec2Rates(e, rs.IType, i.plat)-c)/c*i.arate*730*12), 'g', -1, 32)
					}
				}
				res <- []string{ // emit instance resource variant
					i.id,
					i.name,
					fmt.Sprintf("EC2:%v", rref),
					fmt.Sprintf("%v %v %v", rs.Descr, i.itype, platFmt(i.plat, " instance")),
					eref,
					e.reg,
					e.tref,
					vt,
					i.itype,
					rs.IType,
					strconv.FormatFloat(float64(i.arate*730*12), 'g', -1, 32),
					cv,
				}

				for mount, v := range i.vols {
					vs := rs.Vols[mount]
					if vs == nil {
						res <- []string{ // emit unknown excess volume resource variant
							v.id,
							i.name,
							fmt.Sprintf("EBS:%v:%v", rref, mount),
							fmt.Sprintf("%v %vGiB %v volume", rs.Descr, v.gib, v.stype),
							eref,
							e.reg,
							e.tref,
							"(excess vol)",
							"",
							"",
							strconv.FormatFloat(float64(v.rate*730*12), 'g', -1, 32),
							strconv.FormatFloat(float64(-v.rate*730*12), 'g', -1, 32),
						}
						continue
					}
					vt, cv, gib, iops := "vol type ok", "0", v.gib, float32(0)
					if v.stype != vs.SType {
						vt, cv = "vol type", strconv.FormatFloat(float64((ebsRates(e, vs.SType, v.gib, v.iops)-v.rate)*730*12), 'g', -1, 32)
					}
					res <- []string{ // emit storage type volume resource variant
						v.id,
						i.name,
						fmt.Sprintf("EBS:%v:%v", rref, mount),
						fmt.Sprintf("%v %vGiB volume", rs.Descr, v.gib),
						eref,
						e.reg,
						e.tref,
						vt,
						v.stype,
						vs.SType,
						strconv.FormatFloat(float64(v.rate*730*12), 'g', -1, 32),
						cv, // type->GiB->IOPS correction order assumed
					}
					if vt, cv = "vol GiB ok", "0"; v.gib < vs.GiB[0] {
						gib = vs.GiB[0]
						vt, cv = "vol GiB", strconv.FormatFloat(float64((ebsRates(e, vs.SType, gib, v.iops)-ebsRates(e, vs.SType, v.gib, v.iops))*730*12), 'g', -1, 32)
					} else if v.gib > vs.GiB[1] {
						gib = vs.GiB[1]
						vt, cv = "vol GiB", strconv.FormatFloat(float64((ebsRates(e, vs.SType, gib, v.iops)-ebsRates(e, vs.SType, v.gib, v.iops))*730*12), 'g', -1, 32)
					}
					res <- []string{ // emit GiB size volume resource variant
						v.id,
						i.name,
						fmt.Sprintf("EBS:%v:%v", rref, mount),
						fmt.Sprintf("%v %v volume", rs.Descr, v.stype),
						eref,
						e.reg,
						e.tref,
						vt,
						fmt.Sprintf("%v", v.gib),
						fmt.Sprintf("%v", gib),
						"0", // base cost in "vol type" record
						cv,  // type->GiB->IOPS correction order assumed
					}
					if vt, cv, iops = "vol IOPS ok", "0", adjIOPS(vs, gib); v.iops != iops {
						vt, cv = "vol IOPS", strconv.FormatFloat(float64((ebsRates(e, vs.SType, gib, iops)-ebsRates(e, vs.SType, gib, v.iops))*730*12), 'g', -1, 32)
					}
					res <- []string{ // emit IOPS volume resource variant
						v.id,
						i.name,
						fmt.Sprintf("EBS:%v:%v", rref, mount),
						fmt.Sprintf("%v %vGiB %v volume", rs.Descr, v.gib, v.stype),
						eref,
						e.reg,
						e.tref,
						vt,
						fmt.Sprintf("%v", v.iops),
						fmt.Sprintf("%v", iops),
						"0", // base cost in "vol type" record
						cv,  // type->GiB->IOPS correction order assumed
					}
				}
				for mount, vs := range rs.Vols {
					if i.vols[mount] == nil && vs.GiB[0] > 0 {
						res <- []string{ // emit missing volume resource variant
							"",
							i.name,
							fmt.Sprintf("EBS:%v:%v", rref, mount),
							fmt.Sprintf("%v %vGiB %v volume", rs.Descr, vs.GiB[0], vs.SType),
							eref,
							e.reg,
							e.tref,
							"(missing vol)",
							"",
							"",
							"0",
							strconv.FormatFloat(float64(ebsRates(e, vs.SType, vs.GiB[0], adjIOPS(vs, vs.GiB[0]))*730*12), 'g', -1, 32),
						}
					}
				}
			}
		}

		for rref, mm := range mmm {
			if rs, is, cv := settings.Variance.EC2[rref], e.ec2[rref], ""; rs != nil {
				cf := func() float32 {
					c := ec2Rates(e, rs.IType, rs.Plat)
					for _, vs := range rs.Vols {
						c += ebsRates(e, vs.SType, vs.GiB[0], adjIOPS(vs, vs.GiB[0]))
					}
					return c
				}
				if mm[1] > 0 {
					for o := len(is); o > mm[1]; o-- {
						if cv == "" {
							cv = strconv.FormatFloat(float64(-cf()*730*12), 'g', -1, 32)
						}
						res <- []string{ // emit "imperfect" excess resource variant
							fmt.Sprintf("[%v]", o),
							"",
							fmt.Sprintf("EC2:%v", rref),
							fmt.Sprintf("%v %v", rs.Descr, platFmt(rs.Plat, " instance")),
							eref,
							e.reg,
							e.tref,
							"(excess)",
							"",
							"",
							"0", // already counted
							cv,  // min GiB stands in for volume size of indeterminate excess resources
						}
					}
				}
				for o := len(is); o < mm[0]; o++ {
					if cv == "" {
						cv = strconv.FormatFloat(float64(cf()*730*12), 'g', -1, 32)
					}
					res <- []string{ // emit missing resource variant
						fmt.Sprintf("[%v]", o+1),
						"",
						fmt.Sprintf("EC2:%v", rref),
						fmt.Sprintf("%v %v %v with %v volumes", rs.Descr, rs.IType, platFmt(rs.Plat, " instance"), len(rs.Vols)),
						eref,
						e.reg,
						e.tref,
						"(missing)",
						"",
						"",
						"0",
						cv,
					}
				}
			}
		}
	}
}

func recent(since, recent, last int, ap []int) float32 {
	if len(ap) < 2 {
		return 0
	} else if recent < since {
		recent = since
	}
	var a int
	for o := len(ap) - 1; o > 0 && ap[o] > recent; o -= 2 {
		if ap[o-1] > recent {
			a += ap[o] - ap[o-1] + 1
		} else {
			a += ap[o] - recent + 1
		}
	}
	return float32(a) / float32(last-recent+1)
}
func getPT(t string) string {
	if s := strings.SplitN(t, ".", 2); len(s) == 2 && len(s[0]) > 0 && len(s[1]) > 1 {
		return s[0][:1] + s[1][:2]
	}
	return t
}
func setPT(pt string) func(string) float64 {
	scale := func(pt string) (gen float64, mem float64) {
		if len(pt) > 2 {
			switch pt[1:] {
			case "na", "mi", "sm":
			case "me":
				gen = 0.5
			case "la":
				gen = 1
			case "xl":
				gen = 2
			case "2x":
				gen = 3
			case "4x":
				gen = 4
			case "6x":
				gen = 4.5
			case "8x":
				gen = 5
			case "9x":
				gen = 5.125
			case "12":
				gen = 5.5
			case "16":
				gen = 6
			case "24":
				gen = 6.5
			case "32":
				gen = 7
			default:
				return
			}
			switch pt[0] {
			case 'c':
			case 't', 'm':
				mem = 1
			case 'x':
				mem = 4
			default:
				mem = 2
			}
		}
		return
	}
	ptg, ptm := scale(pt)
	return func(cand string) float64 {
		g, m := scale(cand)
		return (g - ptg) + (m-ptm)*0.6
	}
}
func varianceExtract(rows int, nofilter bool) (res chan []string, err error) {
	var ec2, ebs *modAcc
	var step string
	if rows++; rows < 0 || rows == 1 || rows > maxTableRows+1 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if ec2 = mMod["ec2.aws"].newAcc(); ec2 == nil {
		return nil, fmt.Errorf("\"ec2.aws\" model not found")
	} else if ebs = mMod["ebs.aws"].newAcc(); ebs == nil {
		return nil, fmt.Errorf("\"ebs.aws\" model not found")
	}

	res, step = make(chan []string, 32), "building variance structure"
	go func() {
		defer func() {
			ec2.rel()
			ebs.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while %v: %v", step, e)
				defer recover()
				close(res)
			}
		}()
		scan, tags := make(map[string]*varexEnv, 1024), globalTags(0, 0)
		for tref, t := range settings.Variance.Templates { // build scan map from environments in settings templates
			for eref := range t.Envs {
				if eref != "" {
					e := varexEnv{tref: tref, ec2: make(map[string][]varexInst, 32)}
					scan[eref] = &e
				}
			}
		}

		step = "scanning EC2 instances" // update scan map with current instances
		ec2.reqR()
		for id, inst := range ec2.m.data[1].(*ec2Detail).Inst {
			switch inst.State {
			case "terminated":
			default:
				tag := cmon.TagMap{}.UpdateR(tags[id]).UpdateP(settings.AWS.Accounts[inst.Acct], "cmon:").UpdateV(settings, inst.Acct)
				eref, name := tag["cmon:Env"], tag["cmon:Name"]
				if e, match := scan[eref], "~"; e != nil {
					if e.reg == "" { // assume all environment resouces in 1 region
						e.reg = aws.Region(inst.AZ)
					}
					t := settings.Variance.Templates[e.tref]
					if es := t.Envs[eref]; !nofilter && (es.Nre != nil && es.Nre.FindString(name) != "" ||
						es.Vre != nil && es.Vre.FindString(inst.VPC) != "") {
						continue // skip instances per configured filters
					} else if name != "" {
						rrs := make([]string, 0, 16)
						func(rms []map[string][]int) { // match instance to template resource type
							for _, rm := range rms {
								for rref := range rm {
									if r := settings.Variance.EC2[rref]; r != nil {
										if r.Plat == inst.Plat && (r.Mre != nil && r.Mre.FindString(name) != "" || r.Match == name) {
											if match = rref; r.IType == inst.Typ {
												return
											}
											rrs = append(rrs, rref) // collect candidate matches
										}
									}
								}
							}
							switch len(rrs) {
							case 0, 1:
							default: // use proximate instance type & scale to select from among multiple candidate matches
								pt := getPT(inst.Typ)
								diff, cand, min := setPT(pt), "", 1e2
								for _, rref := range rrs {
									if cand = getPT(settings.Variance.EC2[rref].IType); cand == pt {
										match = rref
										return
									}
									if d := math.Abs(diff(cand)); d < min {
										match, min = rref, d
									}
								}
							}
						}([]map[string][]int{t.EC2, es.EC2})
					}
					e.ec2[match] = append(e.ec2[match], varexInst{
						id:    id,
						name:  name,
						itype: inst.Typ,
						plat:  inst.Plat,
						arate: inst.ORate * recent(inst.Since, inst.Last-3600*720, inst.Last, inst.Active) * settings.AWS.UsageAdj,
						vols:  make(map[string]*varexVol),
					})
				}
			}
		}
		ec2.rel()

		step = "scanning EBS volumes"              // update instances in scan map with mounted volumes
		rloc := make(map[string]*varexInst, 16384) // build temp resource locator
		for _, e := range scan {
			for _, rs := range e.ec2 {
				for o, r := range rs {
					rloc[r.id] = &rs[o]
				}
			}
		}
		ebs.reqR()
		for id, vol := range ebs.m.data[1].(*ebsDetail).Vol {
			if ms := strings.SplitN(vol.Mount, ":", 3); len(ms) > 2 {
				if r := rloc[ms[0]]; r != nil {
					mount, _ := strings.CutPrefix(ms[1], "/dev/")
					r.vols[mount] = &varexVol{
						id:    id,
						stype: vol.Typ,
						gib:   float32(vol.GiB),
						iops:  float32(vol.IOPS),
						rate:  vol.Rate,
					}
				}
			}
		}
		ebs.rel()

		step = "emitting variants" // emit variants to settings in scan map as CSV to res channel
		variance(rows, scan, res)
		close(res)
	}()
	return
}
