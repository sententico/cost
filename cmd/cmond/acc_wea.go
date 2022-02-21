package main

import (
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/cmon"
	"github.com/sententico/cost/tel"
)

const (
	maxTableRows = 2e6 // maximum table extract rows allowed
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
	aspN = regexp.MustCompile(`\b(?P<cust>[a-z][a-z\d-]{1,11})(?P<version>a[12]?\da|b[12]?\db)[tsmlxy][gbewh](?P<app>[a-z]{0,8}?)[\dpr]?\b`)
	// filter criteria operators...
	//  = equals		! not equals
	//  < less/before	> greater/after
	//  [ prefix		] suffix
	//  ~ regex match	^ regex non-match
	fltC = regexp.MustCompile(`^\s*(?P<col>\w[ \w%]*?)\s*(?P<op>[=!<>[\]~^])(?P<opd>.*)$`)
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

func active(since, last int, ap []int) float32 {
	var a int
	for i := 0; i+1 < len(ap); i += 2 {
		a += ap[i+1] - ap[i] + 1
	}
	return float32(a) / float32(last-since+1)
}

func nTags(name string) (t cmon.TagMap) {
	switch settings.Unit {
	case "cmon-aspect", "cmon-alvaria":
		if v := aspN.FindStringSubmatch(name); v != nil {
			t = make(cmon.TagMap)
			for i, k := range aspN.SubexpNames()[1:] {
				t[k] = v[i+1]
			}
		}
	}
	return
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

func (d *ec2Detail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	var adj int
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Acct", "account", "acct":
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
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "AZ", "az":
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
		case "AMI", "ami":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AMI == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).AMI != opd })
			}
		case "Spot", "spot":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Spot == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Spot != opd })
			}
		case "Name", "env", "dc", "product", "app", "cust", "team", "version":
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
		case "State", "state", "stat", "st":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).State != opd })
			}
		case "Since", "since":
			if s := atos(opd); s > 0 {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Since > int(s) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Active%", "active%", "act%":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Active", "active", "act":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Rate", "rate", "ra":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).Rate > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "ORate", "orate", "ora":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).ORate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ec2Item).ORate > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
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
	pg := smPage
	acc.reqR()
	for id, inst := range d.Inst {
		if inst.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.Update(inst.Tag).Update(nTags(inst.Tag["Name"])).UpdateT("team", inst.Tag["SCRM_Group"])
		if tag.Update(settings.AWS.Accounts[inst.Acct]); inst.AZ != "" {
			tag.Update(settings.AWS.Regions[inst.AZ[:len(inst.AZ)-1]])
		}
		if skip(flt, inst, tag) {
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
			inst.AMI,
			inst.Spot,
			tag["Name"],
			tag["env"],
			tag["dc"],
			tag["product"],
			tag["app"],
			tag["cust"],
			tag["team"],
			tag["version"],
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
	acc.rel()
}

func (d *ebsDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	var adj int
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Acct", "account", "acct":
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
		case "Size", "size", "siz":
			if n, err := strconv.Atoi(opd); err == nil {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Size == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Size != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Size < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Size > n })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "IOPS", "iops":
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "AZ", "az":
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
		case "Name", "env", "dc", "product", "app", "cust", "team", "version":
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
		case "State", "state", "stat", "st":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).State != opd })
			}
		case "Since", "since":
			if s := atos(opd); s > 0 {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Since > int(s) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Active%", "active%", "act%":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Active", "active", "act":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Rate", "rate", "ra":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*ebsItem).Rate > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
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
	itags := make(map[string]cmon.TagMap, 4096)
	if ec2 := mMod["ec2.aws"].newAcc(); ec2 != nil && len(ec2.m.data) > 1 {
		acc.reqR()
		for _, vol := range d.Vol {
			if vol.Last >= cur && strings.HasPrefix(vol.Mount, "i-") {
				itags[strings.SplitN(vol.Mount, ":", 2)[0]] = nil
			}
		}
		acc.rel()
		func() {
			ec2.reqR()
			defer ec2.rel()
			for id := range itags {
				if inst := ec2.m.data[1].(*ec2Detail).Inst[id]; inst != nil {
					itags[id] = cmon.TagMap{}.Update(inst.Tag).Update(nTags(inst.Tag["Name"])).UpdateT("team", inst.Tag["SCRM_Group"])
				}
			}
		}()
	}

	pg := smPage
	acc.reqR()
	for id, vol := range d.Vol {
		if vol.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.Update(vol.Tag).Update(nTags(vol.Tag["Name"])).Update(itags[strings.SplitN(vol.Mount, ":", 2)[0]]).UpdateT("team", vol.Tag["SCRM_Group"])
		if tag.Update(settings.AWS.Accounts[vol.Acct]); vol.AZ != "" {
			tag.Update(settings.AWS.Regions[vol.AZ[:len(vol.AZ)-1]])
		}
		if skip(flt, vol, tag) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			vol.Acct + " " + settings.AWS.Accounts[vol.Acct]["~name"],
			vol.Typ,
			strconv.FormatInt(int64(vol.Size), 10),
			strconv.FormatInt(int64(vol.IOPS), 10),
			vol.AZ,
			vol.Mount,
			tag["Name"],
			tag["env"],
			tag["dc"],
			tag["product"],
			tag["app"],
			tag["cust"],
			tag["team"],
			tag["version"],
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
	acc.rel()
}

func (d *rdsDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	var adj int
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Acct", "account", "acct":
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
		case "Sto", "storage", "sto", "stype", "styp":
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
		case "Size", "size", "siz":
			if n, err := strconv.Atoi(opd); err == nil {
				switch op {
				case "=":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Size == n })
				case "!":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Size != n })
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Size < n })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Size > n })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "IOPS", "iops":
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "Engine", "engine", "eng":
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
		case "Name", "env", "dc", "product", "app", "cust", "team", "version":
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
		case "State", "state", "stat", "st":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).State == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).State != opd })
			}
		case "Since", "since":
			if s := atos(opd); s > 0 {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Since > int(s) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Active%", "active%", "act%":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Active", "active", "act":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Rate", "rate", "ra":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*rdsItem).Rate > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
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
	var name, az string
	pg := smPage
	acc.reqR()
	for id, db := range d.DB {
		var tag cmon.TagMap
		if db.Last < cur {
			continue
		} else if name = db.Tag["Name"]; name == "" {
			s := strings.Split(id, ":")
			name = s[len(s)-1]
			tag.UpdateT("Name", name)
		}
		if az = db.AZ; db.MultiAZ {
			az += "+"
		}
		tag = tag.Update(db.Tag).Update(nTags(name)).UpdateT("team", db.Tag["SCRM_Group"])
		if tag.Update(settings.AWS.Accounts[db.Acct]); db.AZ != "" {
			tag.Update(settings.AWS.Regions[db.AZ[:len(db.AZ)-1]])
		}
		if skip(flt, db, az, tag) {
			continue
		} else if rows--; rows == 0 {
			break
		}

		row := []string{
			id,
			db.Acct + " " + settings.AWS.Accounts[db.Acct]["~name"],
			db.Typ,
			db.STyp,
			strconv.FormatInt(int64(db.Size), 10),
			strconv.FormatInt(int64(db.IOPS), 10),
			db.Engine,
			db.Ver,
			db.Lic,
			az,
			name,
			tag["env"],
			tag["dc"],
			tag["product"],
			tag["app"],
			tag["cust"],
			tag["team"],
			tag["version"],
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
	acc.rel()
}

func (d *snapDetail) filters(criteria []string) (int, []func(...interface{}) bool, error) {
	var ct []string
	var adj int
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return 0, nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Acct", "account", "acct":
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
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Size < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Size > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "VSiz", "vsize", "vsiz", "vs":
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "Reg", "region", "reg":
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
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Vol == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Vol != opd })
			}
		case "Par", "parent", "par":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Par == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Par != opd })
			}
		case "Desc", "description", "desc":
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
		case "Name", "env", "dc", "product", "app", "cust", "team", "version":
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
			if s := atos(opd); s > 0 {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Since < int(s) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Since > int(s) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Rate", "rate", "ra":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Rate < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*snapItem).Rate > float32(f) })
				}
			} else {
				return 0, nil, fmt.Errorf("%q operand %q is non-float", c, opd)
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

func (d *snapDetail) table(acc *modAcc, res chan []string, rows, cur int, flt []func(...interface{}) bool) {
	pg := smPage
	acc.reqR()
	for id, snap := range d.Snap {
		if snap.Last < cur {
			continue
		}
		tag := cmon.TagMap{}.Update(snap.Tag).Update(nTags(snap.Tag["Name"])).UpdateT("team", snap.Tag["SCRM_Group"]).Update(
			settings.AWS.Accounts[snap.Acct]).Update(settings.AWS.Regions[snap.Reg])
		if skip(flt, snap, tag) {
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
			tag["Name"],
			tag["env"],
			tag["dc"],
			tag["product"],
			tag["app"],
			tag["cust"],
			tag["team"],
			tag["version"],
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
	acc.rel()
}

func (d *hiD) filters(criteria []string) ([]func(...interface{}) bool, error) {
	var ct []string
	flt := make([]func(...interface{}) bool, 0, 32)
	for nc, c := range criteria {
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Loc", "loc":
			var sl tel.SLmap
			switch sl.Load(nil); op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return sl.Name(v[0].(*cdrItem).Info>>locShift) == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return sl.Name(v[0].(*cdrItem).Info>>locShift) != opd })
			}
		case "To", "to":
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
			if s := atos(opd); s > 0 {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(int64)+int64(v[0].(*cdrItem).Time&offMask) < s })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(int64)+int64(v[0].(*cdrItem).Time&offMask) > s })
				}
			} else {
				return nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Min", "min", "dur":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return float32(v[0].(*cdrItem).Time>>durShift)/600 < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return float32(v[0].(*cdrItem).Time>>durShift)/600 > float32(f) })
				}
			} else {
				return nil, fmt.Errorf("%q operand %q isn't a timestamp", c, opd)
			}
		case "Tries", "tries":
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "Billable", "billable", "bill":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Bill < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Bill > float32(f) })
				}
			} else {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		case "Margin", "margin", "marg":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Marg < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[0].(*cdrItem).Marg > float32(f) })
				}
			} else {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
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
	acc.rel()
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
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "AWS Account", "account", "acct":
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
		case "Resource ID", "rid":
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
		case "Name", "name":
			switch op {
			case "=":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Name == opd })
			case "!":
				flt = append(flt, func(v ...interface{}) bool { return v[0].(*curItem).Name != opd })
			case "[":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasPrefix(v[0].(*curItem).Name, opd) })
			case "]":
				flt = append(flt, func(v ...interface{}) bool { return strings.HasSuffix(v[0].(*curItem).Name, opd) })
			case "~":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Name) != "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			case "^":
				if re, err := regexp.Compile(opd); err == nil {
					flt = append(flt, func(v ...interface{}) bool { return re.FindString(v[0].(*curItem).Name) == "" })
				} else {
					return nil, fmt.Errorf("%q regex operand %q is invalid", c, opd)
				}
			}
		case "env", "dc", "product", "app", "cust", "team", "version":
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
		case "Recs", "recs", "Usage", "usage", "usg":
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
		if ct = fltC.FindStringSubmatch(c); len(ct) <= 3 {
			return nil, fmt.Errorf("invalid criteria syntax: %q", c)
		}
		col, op, opd := ct[1], ct[2], ct[3]
		switch col {
		case "Recs", "recs":
			if n, err := strconv.Atoi(opd); err == nil {
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
			} else {
				return nil, fmt.Errorf("%q operand %q is non-integer", c, opd)
			}
		case "Usage", "usage", "usg":
			if f, err := strconv.ParseFloat(opd, 32); err == nil {
				switch op {
				case "<":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(float32) < float32(f) })
				case ">":
					flt = append(flt, func(v ...interface{}) bool { return v[1].(float32) > float32(f) })
				default:
					return nil, fmt.Errorf("%q operator not supported for %q column", op, col)
				}
			} else {
				return nil, fmt.Errorf("%q operand %q is non-float", c, opd)
			}
		}
	}
	return flt, nil
}

func (d *curDetail) table(li *curItem, from, to int32, un int16, tr float32, id string, tag cmon.TagMap, dts string, flt []func(...interface{}) bool) func() []string {
	var husg func(int32) float32
	var rate float32
	if un < 720 {
		switch rate = li.Cost / li.Usg; {
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
	acct := li.Acct + " " + settings.AWS.Accounts[li.Acct]["~name"]

	return func() []string {
		var rec int16
		var usg, cost float32
		if from > to {
			return nil
		}
		switch un {
		case 1: // hourly
			for rec = 1; ; {
				if usg = husg(from); usg != 0 {
					if cost = usg * rate; (cost > tr || -tr > cost) && !skip(flt, rec, usg) {
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
				if cost = usg * rate; (cost > tr || -tr > cost) && !skip(flt, rec, usg) {
					dts = dts[:8] + fmt.Sprintf("%02d", day/24+1)
					break
				} else if from > to {
					return nil
				}
			}
		default: // monthly
			if rec, usg, cost, from = int16(li.Recs>>recsShift+1), li.Usg, li.Cost, to+1; skip(flt, rec, usg) {
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
			li.Name,
			tag["env"],
			tag["dc"],
			tag["product"],
			tag["app"],
			tag["cust"],
			tag["team"],
			tag["version"],
			strconv.FormatInt(int64(rec), 10),
			strconv.FormatFloat(float64(usg), 'g', -1, 32),
			strconv.FormatFloat(float64(cost), 'g', -1, 32),
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
		acc.reqR()
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
		pg, trunc := smPage, float32(truncate)

		vinst, itags := make(map[string]string, 8192), make(map[string]cmon.TagMap, 4096)
		if ebs, ec2 := mMod["ebs.aws"].newAcc(), mMod["ec2.aws"].newAcc(); ebs != nil && ec2 != nil && len(ebs.m.data) > 1 && len(ec2.m.data) > 1 {
			acc.reqR()
			for mo, hrs := range cur.Month {
				if to >= hrs[0] && hrs[1] >= from {
					for _, li := range cur.Line[mo] {
						if (li.Cost > trunc || -trunc > li.Cost) && strings.HasPrefix(li.RID, "vol-") {
							vinst[li.RID] = ""
						}
					}
				}
			}
			acc.rel()
			func() {
				ebs.reqR()
				defer ebs.rel()
				var inst string
				for id := range vinst {
					if vol := ebs.m.data[1].(*ebsDetail).Vol[id]; vol != nil {
						if strings.HasPrefix(vol.Mount, "i-") {
							inst = strings.SplitN(vol.Mount, ":", 2)[0]
							vinst[id], itags[inst] = inst, nil
						}
					}
				}
			}()
			func() {
				ec2.reqR()
				defer ec2.rel()
				for id := range itags {
					if inst := ec2.m.data[1].(*ec2Detail).Inst[id]; inst != nil {
						itags[id] = cmon.TagMap{}.Update(inst.Tag).Update(nTags(inst.Tag["Name"])).UpdateT("team", inst.Tag["SCRM_Group"])
					}
				}
			}()
		} // use itags map to supplement CUR tags for EBS volumes mapped to instances/itags by vinst

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
					if li.Cost <= trunc && -trunc <= li.Cost {
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
					if tag := (cmon.TagMap{
						"env":     li.Env,
						"dc":      li.DC,
						"product": li.Prod,
						"app":     li.App,
						"cust":    li.Cust,
						"team":    li.Team, // TODO: make SCRM_Group tag available as default?
						"version": li.Ver,
					}).Update(nTags(li.Name)).Update(nTags(li.RID)).Update(itags[vinst[li.RID]]).Update(
						settings.AWS.Accounts[li.Acct]).Update(settings.AWS.Regions[li.Reg]); skip(flt, li, tag) {
						continue
					} else if item := cur.table(li, ifr, ito, units, trunc, id, tag, dts, rflt); item != nil {
						for row := item(); row != nil; row = item() {
							if rows--; rows == 0 {
								break outerLoop
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
			}
		}
		acc.rel()
		close(res)
	}()
	return
}
