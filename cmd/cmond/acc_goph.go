package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
	"github.com/sententico/cost/cmon"
	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/tel"
)

const (
	curItemMin = 3.75e-7 // minimum CUR line item cost for retention (0<curItemMin<curItemDet)
	curItemDet = 0.05    // minimum CUR line item cost to keep hourly usage detail

	rangeShift = 32 - 10 // CUR hour map range (hours - 1)
	usgShift   = 22 - 12 // CUR hour map usage reference (index/value)
	usgMask    = 0xfff   // CUR hour map usage reference (index/value)
	usgIndex   = 743     // CUR hour map usage reference (<=index, >value+743)
	baseMask   = 0x3ff   // CUR hour map range base (hour offset in month)

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
	gopherCmd = cmdMap{
		"aws": "goph_aws.py",
		"az":  "goph_az.py",
		"gcs": "goph_gcs.py",
		"k8s": "goph_k8s.py",
		"rax": "goph_rax.py",
		"asp": "goph_asp.py",
		"":    "goph_test.py", // default gopher
		"~":   "gopher",       // command type
	}
)

func fetch(acc *modAcc, insert func(*modAcc, map[string]string, int), meta bool) (items int) {
	start, now, pages := int(time.Now().Unix()), 0, 0
	csvout := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	defer func() {
		if r := recover(); r != nil {
			if acc.rel() {
				csvout.Close()
			}
			logE.Printf("gopher error while fetching %q: %v", acc.m.name, r)
		}
		if items > 0 {
			if !meta {
				defer func() { recover(); acc.rel() }()
				acc.reqW()
				insert(acc, nil, start)
			}
			logI.Printf("gopher fetched %v items in %v pages for %q", items, pages, acc.m.name)
		}
	}()
	if gophin, gophout, err := gopherCmd.new(acc.m.name, nil); err != nil {
		panic(err)
	} else if err = gophin.Close(); err != nil {
		gophout.Close()
		panic(err)
	} else if err = csvout.Open(gophout); err != nil {
		gophout.Close()
		panic(err)
	}

	var pg int16
	results, errors := csvout.Get()
	for item := range results {
		now, pg = int(time.Now().Unix()), smPage
		pages++
		for acc.reqW(); ; {
			if item["~meta"] == "" {
				insert(acc, item, now)
				items++
			} else if meta {
				insert(acc, item, now)
			}
			if pg--; pg > 0 {
				select {
				case item = <-results:
					if item != nil {
						continue
					}
				default:
				}
			}
			acc.rel()
			break
		}
	}
	csvout.Close()
	if err := <-errors; err != nil {
		panic(err)
	}
	return
}

// ec2.aws model gopher accessors
//
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
func ec2awsInsert(acc *modAcc, item map[string]string, now int) {
	sum, detail, work, id := acc.m.data[0].(*ec2Sum), acc.m.data[1].(*ec2Detail), acc.m.data[2].(*ec2Work), item["id"]
	if item == nil {
		if now > detail.Current {
			detail.Current = now
		}
		return
	} else if id == "" {
		return
	}
	inst, initialized := detail.Inst[id]
	if !initialized {
		inst = &ec2Item{
			Typ:   item["type"],
			Plat:  item["plat"],
			AMI:   item["ami"],
			Spot:  item["spot"],
			Since: now,
		}
		detail.Inst[id] = inst
	}
	inst.Acct = item["acct"]
	inst.Vol = atoi(item["vol"], 0)
	inst.AZ = item["az"]
	if tag := item["tag"]; tag != "" {
		inst.Tag = make(cmon.TagMap)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			inst.Tag[kvs[0]] = kvs[1]
		}
	} else {
		inst.Tag = nil
	}
	if !initialized {
		ec2awsHack(inst)
	}

	switch inst.State = item["state"]; inst.State {
	case "running":
		reg := aws.Region(inst.AZ)
		k := aws.RateKey{
			Region: reg,
			Typ:    inst.Typ,
			Plat:   inst.Plat,
		}
		if r := work.rates.Lookup(&k); r.Rate == 0 {
			logE.Printf("no EC2 %v rate found for %v/%v in %v", k.Terms, k.Typ, k.Plat, reg)
			inst.Rate = inst.ORate * settings.AWS.UsageAdj
		} else if inst.ORate != 0 {
			inst.Rate = inst.ORate * settings.AWS.UsageAdj
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
			c := inst.Rate * float32(dur) / 3600
			sum.Current = sum.ByAcct.add(now, int(dur), inst.Acct, dur, c)
			sum.ByRegion.add(now, int(dur), reg, dur, c)
			if inst.Spot == "" {
				sum.BySKU.add(now, int(dur), reg+" "+k.Typ+" "+inst.Plat, dur, c)
			} else {
				sum.BySKU.add(now, int(dur), reg+" sp."+k.Typ+" "+inst.Plat, dur, c)
			}
		}
	default:
		inst.Rate = 0
	}
	inst.Last = now
}

// ebs.aws model gopher accessors
//
func ebsawsInsert(acc *modAcc, item map[string]string, now int) {
	sum, detail, work, id := acc.m.data[0].(*ebsSum), acc.m.data[1].(*ebsDetail), acc.m.data[2].(*ebsWork), item["id"]
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
			Since: now,
		}
		detail.Vol[id] = vol
	} else {
		dur = now - vol.Last
	}
	vol.Acct = item["acct"]
	vol.Typ = item["type"]
	vol.Size = atoi(item["size"], 0)
	vol.IOPS = atoi(item["iops"], 0)
	vol.AZ = item["az"]
	if vol.Mount = item["mount"]; vol.Mount == "0 attachments" {
		vol.Mount = ""
	}
	if tag := item["tag"]; tag != "" {
		vol.Tag = make(cmon.TagMap)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			vol.Tag[kvs[0]] = kvs[1]
		}
	} else {
		vol.Tag = nil
	}

	reg := aws.Region(vol.AZ)
	k, c := aws.EBSRateKey{
		Region: reg,
		Typ:    vol.Typ,
	}, float32(0)
	r := work.rates.Lookup(&k)
	if r.SZrate == 0 {
		logE.Printf("no EBS rate found for %v in %v", k.Typ, reg)
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
		switch vol.Typ {
		case "gp3":
			if vol.IOPS > 3000 {
				vol.Rate, c = (r.SZrate*float32(vol.Size)+r.IOrate*float32(vol.IOPS-3000))*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
			} else {
				vol.Rate, c = r.SZrate*float32(vol.Size)*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
			}
		default:
			vol.Rate, c = (r.SZrate*float32(vol.Size)+r.IOrate*float32(vol.IOPS))*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
		}
	default:
		vol.Rate = 0
	}
	if c > 0 {
		u := uint64(vol.Size * dur)
		sum.Current = sum.ByAcct.add(now, dur, vol.Acct, u, c)
		sum.ByRegion.add(now, dur, reg, u, c)
		sum.BySKU.add(now, dur, reg+" "+k.Typ, u, c)
	}
	vol.Last = now
}

// rds.aws model gopher accessors
//
func rdsawsInsert(acc *modAcc, item map[string]string, now int) {
	sum, detail, work, id := acc.m.data[0].(*rdsSum), acc.m.data[1].(*rdsDetail), acc.m.data[2].(*rdsWork), item["id"]
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
		db.Tag = make(cmon.TagMap)
		for _, kv := range strings.Split(tag, "\t") {
			kvs := strings.Split(kv, "=")
			db.Tag[kvs[0]] = kvs[1]
		}
	} else {
		db.Tag = nil
	}

	reg := aws.Region(db.AZ)
	k := aws.RateKey{
		Region: reg,
		Typ:    db.Typ,
		Plat:   db.Engine,
	}
	r, s, u, c := work.rates.Lookup(&k), work.srates.Lookup(&aws.EBSRateKey{
		Region: reg,
		Typ:    db.STyp,
	}), uint64(0), float32(0)
	if r.Rate == 0 || s.SZrate == 0 {
		logE.Printf("no RDS %v rate found for %v/%v[%v] in %v", k.Terms, k.Typ, k.Plat, db.STyp, reg)
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
		sum.Current = sum.ByAcct.add(now, dur, db.Acct, u, c)
		sum.ByRegion.add(now, dur, reg, u, c)
		sum.BySKU.add(now, dur, reg+" "+k.Typ+" "+db.Engine, u, c)
	}
	db.Last = now
}

// cur.aws model gopher accessors
//
func curawsFinalize(acc *modAcc) {
	psum, pdet, work, pg := acc.m.data[0].(*curSum), acc.m.data[1].(*curDetail), acc.m.data[2].(*curWork), lgPage
	for mo, wm := range work.idet.Line {
		bt, _ := time.Parse(time.RFC3339, mo[:4]+"-"+mo[4:]+"-01T00:00:00Z")
		bh, eh, pm, tc, tl := int32(bt.Unix())/3600, int32(bt.AddDate(0, 1, 0).Unix()-1)/3600, pdet.Line[mo], 0.0, 0

		for id, line := range wm {
			if line.Cost <= curItemMin && -curItemMin <= line.Cost {
				delete(wm, id)
				tc += float64(line.Cost)
				tl++
			} else if line.Cost < curItemDet && -curItemDet < line.Cost {
				line.HMap, line.HUsg = nil, nil
			} else if len(line.HMap) > 0 { // size-optimize usage history
				hu, min, max, ht, ut, mc := [usgIndex + 2]uint16{}, uint16(usgIndex), uint16(1), 0, uint16(0), 0
				for _, m := range line.HMap { // expand/order map usage references
					r, u, b := uint16(m>>rangeShift), uint16(m>>usgShift&usgMask+1), uint16(m&baseMask)
					if b < min {
						min = b
					}
					for r += b; b <= r; b++ {
						if hu[b] == 0 {
							hu[b] = u // +1 to distinguish from nil usage reference
						}
					}
					if b > max {
						max = b
					}
				}
				for _, u := range hu[min : max+1] { // count optimal (minimum) usage-grouped range maps
					if u == ut {
						continue
					} else if ut != 0 {
						mc++
					}
					ut = u
				}
				var husg []float32 // build optimal usage representation (un/mapped)
				if int(max-min+1) <= mc+len(line.HUsg) {
					line.HMap, husg = nil, make([]float32, int(max-min+1))
					husg[0] = float32(min)
					for h := uint16(1); h <= max-min; h++ {
						if u := hu[min+h-1]; u > usgIndex+1 {
							husg[h] = float32(u - usgIndex - 1)
						} else if u > 0 {
							husg[h] = line.HUsg[u-1]
						}
					}
				} else if mbuild := func() {
					line.HMap, ut = make([]uint32, 0, mc), 0
					for h, u := range hu[min : max+1] {
						if u == ut {
							continue
						} else if ut != 0 {
							line.HMap = append(line.HMap, uint32((h-ht-1)<<rangeShift|int(ut-1)<<usgShift|int(min)+ht))
						}
						ht, ut = h, u
					}
				}; len(line.HUsg) == cap(line.HUsg) {
					mbuild()
					husg = line.HUsg
				} else {
					mbuild()
					husg = make([]float32, len(line.HUsg))
					copy(husg, line.HUsg)
				}
				line.HUsg = husg
				if pg--; pg <= 0 { // paginate sustained model access
					acc.rel()
					pg = lgPage
					acc.reqW()
				}
			}
		}

		if len(wm) < len(pm)/5*4 {
			logE.Printf("%s AWS CUR update rejected: only %d line items offered to update %d",
				bt.Format("Jan06"), len(wm), len(pm))
		} else {
			pdet.Line[mo], pdet.Month[mo] = wm, &[2]int32{bh, eh}
			psum.ByAcct.update(work.isum.ByAcct, bh, eh)
			psum.ByRegion.update(work.isum.ByRegion, bh, eh)
			psum.ByTyp.update(work.isum.ByTyp, bh, eh)
			psum.BySvc.update(work.isum.BySvc, bh, eh)
			logI.Printf("%s AWS CUR update: %d line items (%d truncated @$%.4f) updated %d",
				bt.Format("Jan06"), len(wm), tl, tc, len(pm))
		}
	}
}
func curawsInsert(acc *modAcc, item map[string]string, now int) {
	work, id := acc.m.data[2].(*curWork), item["id"]
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
		} else if work.isum.ByTyp == nil {
			logE.Printf("AWS CUR input out of context: %q", meta)
		} else if strings.HasPrefix(meta, "section 20") && len(meta) > 14 {
			if t, err := time.Parse(time.RFC3339, meta[8:12]+"-"+meta[12:14]+"-01T00:00:00Z"); err == nil {
				if work.imo = meta[8:14]; work.idet.Line[work.imo] == nil {
					work.idet.Line[work.imo] = make(map[string]*curItem)
				}
				work.ihr = uint32(t.Unix() / 3600)
				work.idetm = work.idet.Line[work.imo]
			} else {
				work.imo = ""
				logE.Printf("unrecognized AWS CUR input section: %q", meta[8:])
			}
		} else if strings.HasPrefix(meta, "end ") {
			curawsFinalize(acc)
			acc.m.data[2] = &curWork{}
		} else if meta != "" {
			logE.Printf("unrecognized AWS CUR input: %q", meta)
		}
		return
	} else if work.imo == "" {
		return
	}

	var h uint32
	if t, err := time.Parse(time.RFC3339, item["hour"]); err == nil {
		if h = uint32(t.Unix()/3600) - work.ihr; h > usgIndex {
			h = 0
		}
	}
	u, _ := strconv.ParseFloat(item["usg"], 32)
	c, _ := strconv.ParseFloat(item["cost"], 64)
	line, hr, r, us, ur, co := work.idetm[id], int32(h+work.ihr), 0, float32(u), uint32(0), float32(c)
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
			Mu:   -1,
		}
		work.idetm[id] = line
	}
	if us <= 0 {
		ur, us = usgIndex+1, 1
	} else if us <= usgMask-usgIndex && us == float32(int32(us)) {
		ur = uint32(us) + usgIndex
	} else {
		for ; ur < uint32(len(line.HUsg)) && line.HUsg[ur] != us; ur++ {
		}
		if ur == uint32(len(line.HUsg)) && ur <= usgIndex {
			line.HUsg = append(line.HUsg, us)
		}
	}
	for ; r < len(line.HMap) && line.HMap[r]&baseMask+line.HMap[r]>>rangeShift+1 != h; r++ {
	}
	if r == len(line.HMap) || line.HMap[r]>>usgShift&usgMask != ur {
		line.HMap = append(line.HMap, ur<<usgShift|h)
	} else {
		line.HMap[r] += 1 << rangeShift
	}

	line.Usg += us
	line.Cost += co
	line.Mu++
	work.isum.ByAcct.add(hr, line.Acct, c)
	work.isum.ByRegion.add(hr, line.Reg, c)
	work.isum.ByTyp.add(hr, line.Typ, c)
	work.isum.BySvc.add(hr, line.Svc, c)
}

// cdr.asp model gopher accessors
//
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
func cdraspInsert(acc *modAcc, item map[string]string, now int) {
	id, tsum, osum := cdrID(ato64(item["id"], 0)), acc.m.data[0].(*termSum), acc.m.data[1].(*origSum)
	tdetail, odetail, work := acc.m.data[2].(*termDetail), acc.m.data[3].(*origDetail), acc.m.data[4].(*cdrWork)
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

	switch typ, ip, itg, etg := item["type"], item["IP"], item["iTG"], item["eTG"]; {
	case typ == "CORE" || itg == "USPRODMBZ_ZIPWIRE_TG" || strings.HasPrefix(etg, "ACCESS_"):
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
		if len(itg) > 6 && itg[:6] == "ASPTIB" {
			cdr.Info |= work.sp.Code(itg[6:]) & spMask
		} else if len(itg) > 5 && itg[:5] == "SUAIB" {
			cdr.Info |= work.sp.Code(itg[5:]) & spMask
		} else if len(itg) > 4 { // BYOC/PBXC
			cdr.Info |= work.sp.Code(itg[:4]) & spMask
		}
		if odetail.CDR.add(hr, cdrID(lc)<<gwlocShift|id&idMask, cdr) {
			if cdr.Info&spMask == 0 {
				work.except["iTG:"+itg]++
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
		if len(item["dip"]) >= 20 && decoder.Full(item["dip"][:10], &work.to) != nil {
			break
		} else if err := decoder.Full(item["to"], &work.to); err != nil {
			// TODO: remove E.164 decoder debug section when validated...
			var sp string
			if len(etg) > 6 && etg[:6] == "ASPTOB" {
				sp = work.sp.Name(work.sp.Code(etg[6:]))
			} else if len(etg) > 4 { // BYOC/PBXC
				sp = work.sp.Name(work.sp.Code(etg[:4]))
			}
			if e := fmt.Sprintf("[%v/%v] %v", work.sl.Name(lc), sp,
				err); !strings.Contains(e, "customer] prefix [0") &&
				!strings.Contains(e, "customer] invalid E.164 filtered") {
				if work.dexcept[e]++; work.dexcept[e] == 1 {
					logE.Printf("%016X%v", id, e)
				}
			}
			// TODO: ...debug section end
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
		if len(etg) > 6 && etg[:6] == "ASPTOB" {
			cdr.Info |= work.sp.Code(etg[6:]) & spMask
		} else if len(etg) > 4 { // BYOC/PBXC
			cdr.Info |= work.sp.Code(etg[:4]) & spMask
		}
		if tdetail.CDR.add(hr, cdrID(lc)<<gwlocShift|id&idMask, cdr) {
			if cdr.Info&spMask == 0 {
				work.except["eTG:"+etg]++
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

// general accessor helpers
//
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
