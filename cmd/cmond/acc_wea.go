package main

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/tel"
)

const (
	pgSize = 512
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

func (sum hsU) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'n':
				rct[n] += float64(i.Usage) / 3600
			default:
				rct[n] += i.Cost
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'n':
							s[h] = float64(i.Usage) / 3600
						default:
							s[h] = i.Cost
						}
						ser[n] = s
					}
				}
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hsA) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			rct[n] += i
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						s = s[:h+1]
						s[h], ser[n] = i, s
					}
				}
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hsC) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'm':
				rct[n] += i.Marg
			case 'c':
				rct[n] += i.Bill - i.Marg
			case 'n':
				rct[n] += float64(i.Calls)
			case 'd':
				rct[n] += float64(i.Dur) / 600
			default:
				rct[n] += i.Bill
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'm':
							s[h] = i.Marg
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
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hnC) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, nser, ser := make(map[tel.E164digest]float64), make(map[tel.E164digest][]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'm':
				rct[n] += i.Marg
			case 'c':
				rct[n] += i.Bill - i.Marg
			case 'n':
				rct[n] += float64(i.Calls)
			case 'd':
				rct[n] += float64(i.Dur) / 600
			default:
				rct[n] += i.Bill
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			nser[n] = make([]float64, 0, span)
		}
	}
	if len(nser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := nser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'm':
							s[h] = i.Marg
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
			} else if h >= recent {
				break
			}
		}
		var e164 tel.E164full
		for n, s := range nser {
			n.Full(nil, &e164)
			ser[fmt.Sprintf("+%v %v[%v]", e164.CC, e164.P, e164.Geo)] = s
		}
	}
	return
}

func seriesExtract(metric string, span, recent int, truncate float64) (res chan map[string][]float64, err error) {
	var acc *modAcc
	var sum interface{}
	var cur int32
	var typ byte
	if span <= 0 || span > 24*90 || recent <= 0 || recent > span || truncate < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(strings.SplitN(metric, "/", 2)[0], ".", 3)[:2], ".")].newAcc(); acc == nil {
		return nil, fmt.Errorf("model not found")
	} else if metric[len(metric)-2] == '/' {
		typ = metric[len(metric)-1]
	}

	switch metric {
	case "ec2.aws/acct", "ec2.aws/acct/s":
		sum, cur = acc.m.data[0].(*ec2Sum).ByAcct, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/region", "ec2.aws/region/s":
		sum, cur = acc.m.data[0].(*ec2Sum).ByRegion, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/sku", "ec2.aws/sku/s":
		sum, cur = acc.m.data[0].(*ec2Sum).BySKU, acc.m.data[0].(*ec2Sum).Current

	case "ebs.aws/acct", "ebs.aws/acct/s":
		sum, cur = acc.m.data[0].(*ebsSum).ByAcct, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/region", "ebs.aws/region/s":
		sum, cur = acc.m.data[0].(*ebsSum).ByRegion, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/sku", "ebs.aws/sku/s":
		sum, cur = acc.m.data[0].(*ebsSum).BySKU, acc.m.data[0].(*ebsSum).Current

	case "rds.aws/acct", "rds.aws/acct/s":
		sum, cur = acc.m.data[0].(*rdsSum).ByAcct, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/region", "rds.aws/region/s":
		sum, cur = acc.m.data[0].(*rdsSum).ByRegion, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/sku", "rds.aws/sku/s":
		sum, cur = acc.m.data[0].(*rdsSum).BySKU, acc.m.data[0].(*rdsSum).Current

	case "cur.aws/acct":
		sum, cur = acc.m.data[0].(*curSum).ByAcct, acc.m.data[0].(*curSum).Current
	case "cur.aws/region":
		sum, cur = acc.m.data[0].(*curSum).ByRegion, acc.m.data[0].(*curSum).Current
	case "cur.aws/typ":
		sum, cur = acc.m.data[0].(*curSum).ByTyp, acc.m.data[0].(*curSum).Current
	case "cur.aws/svc":
		sum, cur = acc.m.data[0].(*curSum).BySvc, acc.m.data[0].(*curSum).Current

	case "cdr.asp/term/cust", "cdr.asp/term/cust/m", "cdr.asp/term/cust/c", "cdr.asp/term/cust/n", "cdr.asp/term/cust/d":
		sum, cur = acc.m.data[0].(*termSum).ByCust, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/geo", "cdr.asp/term/geo/m", "cdr.asp/term/geo/c", "cdr.asp/term/geo/n", "cdr.asp/term/geo/d":
		sum, cur = acc.m.data[0].(*termSum).ByGeo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/sp", "cdr.asp/term/sp/m", "cdr.asp/term/sp/c", "cdr.asp/term/sp/n", "cdr.asp/term/sp/d":
		sum, cur = acc.m.data[0].(*termSum).BySP, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/loc", "cdr.asp/term/loc/m", "cdr.asp/term/loc/c", "cdr.asp/term/loc/n", "cdr.asp/term/loc/d":
		sum, cur = acc.m.data[0].(*termSum).ByLoc, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/to", "cdr.asp/term/to/m", "cdr.asp/term/to/c", "cdr.asp/term/to/n", "cdr.asp/term/to/d":
		sum, cur = acc.m.data[0].(*termSum).ByTo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/from", "cdr.asp/term/from/m", "cdr.asp/term/from/c", "cdr.asp/term/from/n", "cdr.asp/term/from/d":
		sum, cur = acc.m.data[0].(*termSum).ByFrom, acc.m.data[0].(*termSum).Current

	case "cdr.asp/orig/cust", "cdr.asp/orig/cust/m", "cdr.asp/orig/cust/c", "cdr.asp/orig/cust/n", "cdr.asp/orig/cust/d":
		sum, cur = acc.m.data[1].(*origSum).ByCust, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/geo", "cdr.asp/orig/geo/m", "cdr.asp/orig/geo/c", "cdr.asp/orig/geo/n", "cdr.asp/orig/geo/d":
		sum, cur = acc.m.data[1].(*origSum).ByGeo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/sp", "cdr.asp/orig/sp/m", "cdr.asp/orig/sp/c", "cdr.asp/orig/sp/n", "cdr.asp/orig/sp/d":
		sum, cur = acc.m.data[1].(*origSum).BySP, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/loc", "cdr.asp/orig/loc/m", "cdr.asp/orig/loc/c", "cdr.asp/orig/loc/n", "cdr.asp/orig/loc/d":
		sum, cur = acc.m.data[1].(*origSum).ByLoc, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/to", "cdr.asp/orig/to/m", "cdr.asp/orig/to/c", "cdr.asp/orig/to/n", "cdr.asp/orig/to/d":
		sum, cur = acc.m.data[1].(*origSum).ByTo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/from", "cdr.asp/orig/from/m", "cdr.asp/orig/from/c", "cdr.asp/orig/from/n", "cdr.asp/orig/from/d":
		sum, cur = acc.m.data[1].(*origSum).ByFrom, acc.m.data[1].(*origSum).Current

	default:
		return nil, fmt.Errorf("unknown metric")
	}
	res = make(chan map[string][]float64, 1)

	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer func() { recover() }()
				close(res)
			}
		}()
		var ser map[string][]float64
		acc.reqR() // TODO: may relocate prior to acc.m.data reference
		switch sum := sum.(type) {
		case hsU:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hsA:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hsC:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hnC:
			ser = sum.extract(typ, cur, span, recent, truncate)
		}
		acc.rel()
		res <- ser
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

func (d *ec2Detail) extract(acc *modAcc, res chan []string, items int) {
	pg := pgSize
	acc.reqR()
	for id, inst := range d.Inst {
		if inst.Last < d.Current {
			continue
		} else if items--; items == 0 {
			break
		}

		ls := []string{
			id,
			inst.Acct,
			inst.Typ,
			inst.Plat,
			strconv.FormatInt(int64(inst.Vol), 10),
			inst.AZ,
			inst.AMI,
			inst.Spot,
			inst.Tag["env"],
			inst.Tag["dc"],
			inst.Tag["product"],
			inst.Tag["app"],
			inst.Tag["cust"],
			inst.Tag["team"],
			inst.Tag["version"],
			inst.State,
			time.Unix(int64(inst.Since), 0).Format("2006-01-02 15:04:05"),
			time.Unix(int64(inst.Last), 0).Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(inst.Since, inst.Last, inst.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(inst.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- ls:
				continue
			default:
			}
		}
		acc.rel()
		res <- ls
		pg = pgSize
		acc.reqR()
	}
	acc.rel()
}

func (d *ebsDetail) extract(acc *modAcc, res chan []string, items int) {
	pg := pgSize
	acc.reqR()
	for id, vol := range d.Vol {
		if vol.Last < d.Current {
			continue
		} else if items--; items == 0 {
			break
		}

		ls := []string{
			id,
			vol.Acct,
			vol.Typ,
			strconv.FormatInt(int64(vol.Size), 10),
			strconv.FormatInt(int64(vol.IOPS), 10),
			vol.AZ,
			vol.Mount,
			vol.Tag["env"],
			vol.Tag["dc"],
			vol.Tag["product"],
			vol.Tag["app"],
			vol.Tag["cust"],
			vol.Tag["team"],
			vol.Tag["version"],
			vol.State,
			time.Unix(int64(vol.Since), 0).Format("2006-01-02 15:04:05"),
			time.Unix(int64(vol.Last), 0).Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(vol.Since, vol.Last, vol.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(vol.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- ls:
				continue
			default:
			}
		}
		acc.rel()
		res <- ls
		pg = pgSize
		acc.reqR()
	}
	acc.rel()
}

func (d *rdsDetail) extract(acc *modAcc, res chan []string, items int) {
	pg := pgSize
	acc.reqR()
	for id, db := range d.DB {
		if db.Last < d.Current {
			continue
		} else if items--; items == 0 {
			break
		}

		ls := []string{
			id,
			db.Acct,
			db.Typ,
			db.STyp,
			strconv.FormatInt(int64(db.Size), 10),
			db.Engine,
			db.Ver,
			db.Lic,
			db.AZ,
			db.Tag["env"],
			db.Tag["dc"],
			db.Tag["product"],
			db.Tag["app"],
			db.Tag["cust"],
			db.Tag["team"],
			db.Tag["version"],
			db.State,
			time.Unix(int64(db.Since), 0).Format("2006-01-02 15:04:05"),
			time.Unix(int64(db.Last), 0).Format("2006-01-02 15:04:05"),
			strconv.FormatFloat(float64(active(db.Since, db.Last, db.Active)), 'g', -1, 32),
			strconv.FormatFloat(float64(db.Rate), 'g', -1, 32),
		}
		if pg--; pg >= 0 {
			select {
			case res <- ls:
				continue
			default:
			}
		}
		acc.rel()
		res <- ls
		pg = pgSize
		acc.reqR()
	}
	acc.rel()
}

func (d *hiD) extract(acc *modAcc, res chan []string, items int) {
	var to, from tel.E164full
	pg := pgSize
	acc.reqR()
nextHour:
	for h, hm := range *d {
		t := int64(h) * 3600
		for id, cdr := range hm {
			if items--; items == 0 {
				break nextHour
			}

			cdr.To.Full(nil, &to)
			cdr.From.Full(nil, &from)
			ls := []string{
				fmt.Sprintf("%X_%X_%X_%X", id>>gwlocShift, id>>shelfShift&shelfMask, id>>bootShift&bootMask, id&callMask),
				fmt.Sprintf("+%s %s %s %s", to.CC, to.P, to.Sub, to.Geo),
				fmt.Sprintf("+%s %s %s %s", from.CC, from.P, from.Sub, from.Geo),
				cdr.Cust,
				time.Unix(t+int64(cdr.Time&offMask), 0).Format("2006-01-02 15:04:05"),
				strconv.FormatFloat(float64(cdr.Time>>durShift)/600, 'g', -1, 32),
				strconv.FormatFloat(float64(cdr.Bill), 'g', -1, 32),
				strconv.FormatFloat(float64(cdr.Marg), 'g', -1, 32),
				fmt.Sprintf("%X_%X_%X", cdr.Info>>locShift, cdr.Info>>triesShift&triesMask, cdr.Info&spMask),
			}
			if pg--; pg >= 0 {
				select {
				case res <- ls:
					continue
				default:
				}
			}
			acc.rel()
			res <- ls
			pg = pgSize
			acc.reqR()
		}
	}
	acc.rel()
}

func streamExtract(n string, items int) (res chan []string, err error) {
	var acc *modAcc
	if items++; items < 0 || items == 1 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(strings.SplitN(n, "/", 2)[0], ".", 3)[:2], ".")].newAcc(); acc == nil || len(acc.m.data) < 2 {
		return nil, fmt.Errorf("model not found")
	}

	res = make(chan []string, 32)
	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer func() { recover() }()
				close(res)
			}
		}()

		switch det := acc.m.data[1].(type) {
		case *ec2Detail:
			det.extract(acc, res, items)
		case *ebsDetail:
			det.extract(acc, res, items)
		case *rdsDetail:
			det.extract(acc, res, items)
		case *origSum:
			switch n {
			case "cdr.asp/term":
				acc.m.data[2].(*termDetail).CDR.extract(acc, res, items)
			case "cdr.asp/orig":
				acc.m.data[3].(*origDetail).CDR.extract(acc, res, items)
			}
		}
		close(res)
	}()
	return
}

func getSlicer(from, to int32, un int16, tr float32, hrs *[2]int32, id string, li *curItem, dts string) func() []string {
	var husg [744]float32
	var rate float32
	if from -= hrs[0]; from < 0 {
		from = 0
	}
	if to > hrs[1] {
		to = hrs[1]
	}
	if to -= hrs[0]; un < 720 {
		if rate = li.Cost / li.Usg; len(li.Hour) > 0 {
			for i, h := range li.Hour {
				for ho, r := int32(h&baseMask)-hrs[0], int32(h>>rangeShift&rangeMask); r >= 0; r-- {
					husg[ho+r] = li.HUsg[i]
				}
			}
		} else {
			husg[0] = li.Usg
		}
	}

	return func() []string {
		var rec int16
		var usg, cost float32
		switch un {
		case 1:
			for prev := from; ; prev = from {
				if from > to {
					return nil
				} else if from++; husg[prev] != 0 {
					usg = husg[prev]
					if cost = usg * rate; cost >= tr || -tr >= cost {
						rec, dts = 1, dts[:8]+fmt.Sprintf("%02d %02d:00", prev/24+1, prev%24)
						break
					}
				}
			}
		case 24:
			for ; ; rec, usg = 0, 0 {
				if from > to {
					return nil
				}
				for day := from + 23; from <= day; from++ {
					if husg[from] != 0 {
						rec++
						usg += husg[from]
					}
				}
				if cost = usg * rate; cost >= tr || -tr >= cost {
					dts = dts[:8] + fmt.Sprintf("%02d", from/24)
					break
				}
			}
		default:
			if from > to {
				return nil
			}
			rec, usg, cost = li.Mu+1, li.Usg, li.Cost
			from = to + 1
		}
		return []string{
			dts[:8] + id,
			dts,
			li.Acct,
			li.Typ,
			li.Svc,
			li.UTyp,
			li.UOp,
			li.Reg,
			li.RID,
			li.Desc,
			li.Name,
			li.Env,
			li.DC,
			li.Prod,
			li.App,
			li.Cust,
			li.Team,
			li.Ver,
			strconv.FormatInt(int64(rec), 10),
			strconv.FormatFloat(float64(usg), 'g', -1, 32),
			strconv.FormatFloat(float64(cost), 'g', -1, 32),
		}
	}
}

func curawsExtract(from, to int32, units int16, items int, truncate float64) (res chan []string, err error) {
	var acc *modAcc
	if items++; from > to || units < 1 || items < 0 || items == 1 || truncate < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod["cur.aws"].newAcc(); acc == nil {
		return nil, fmt.Errorf("\"cur.aws\" model not found")
	}

	res = make(chan []string, 32)
	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer func() { recover() }()
				close(res)
			}
		}()
		pg, trunc := pgSize, float32(truncate)
		acc.reqR()

	nextMonth:
		for mo, hrs := range acc.m.data[1].(*curDetail).Month {
			if to >= hrs[0] && hrs[1] >= from {
				dts := mo[:4] + "-" + mo[4:] + "-01" // +" "+hh+":00"
				for id, li := range acc.m.data[1].(*curDetail).Line[mo] {
					if li.Cost >= trunc || -trunc >= li.Cost {
						slicer := getSlicer(from, to, units, trunc, hrs, id, li, dts)
						for ls := slicer(); ls != nil; ls = slicer() {
							if items--; items == 0 {
								break nextMonth
							}
							if pg--; pg >= 0 {
								select {
								case res <- ls:
									continue
								default:
								}
							}
							acc.rel()
							res <- ls
							pg = pgSize
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
