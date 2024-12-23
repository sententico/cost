package main

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
	"github.com/sententico/cost/cmon"
	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/tel"
)

const (
	curItemMin = 2e-7 // minimum CUR line item charge to avoid truncation
	curItemDev = 0.02 // deviation limit from mean hourly line item charge to elide usage detail

	rangeShift = 32 - 10 // CUR hour map range (hours - 1)
	usgShift   = 22 - 12 // CUR hour map usage reference (index/value)
	usgMask    = 0xfff   // CUR hour map usage reference (index/value)
	usgIndex   = 743     // CUR hour map usage reference (<=index, >value+743)
	baseMask   = 0x3ff   // CUR hour map range base (hour offset in month)
	hrBitmap   = 0b110   // offset bitmap of mo-hr usage (alt CUR hour map ID; 0b1110,... reserved)
	hrBMShift  = 32 - 3  // offset bitmap of mo-hr usage (initial alt CUR hour map entry non-ID bits)

	recsShift = 32 - 12 // CUR records map multiple (count - 1)
	foffShift = 20 - 10 // CUR records map from (hour offset in month)
	foffMask  = 0x3ff   // CUR records map from (hour offset in month)
	toffMask  = 0x3ff   // CUR records map to (hour offset in month)

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

	snapRefs = regexp.MustCompile(`\b(snap|vol)-([0-9a-f]{8}|[0-9a-f]{17})\b`)
)

func fetch(acc *modAcc, alt string, insert func(*modAcc, map[string]string, int), meta bool) (items int) {
	gopher, start, now, pages := acc.m.name+alt, int(time.Now().Unix()), 0, 0
	csvout := csv.Resource{Typ: csv.RTcsv, Sep: '\t', Comment: "#", Shebang: "#!"}
	defer func() {
		if r := recover(); r != nil {
			if acc.rel() {
				csvout.Close()
			}
			logE.Printf("gopher error while fetching %q: %v", gopher, r)
		}
		if items > 0 {
			if !meta {
				defer func() {
					acc.rel()
					if r := recover(); r != nil {
						logE.Printf("gopher error finalizing %q fetch: %v", gopher, r)
					}
				}()
				acc.reqW()
				insert(acc, nil, start)
			}
			logI.Printf("gopher fetched %v items in %v pages for %q", items, pages, gopher)
		}
	}()
	if gophin, gophout, err := gopherCmd.new(gopher, nil); err != nil {
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

// ec2.aws model gopher accessors...
func ec2awsHack(inst *ec2Item) {
	switch settings.Unit {
	case "cmon-aspect", "cmon-alvaria":
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
			Plat:  item["plat"],
			AMI:   item["ami"],
			Spot:  item["spot"],
			Since: now,
		}
		detail.Inst[id] = inst
	}
	inst.Acct = item["acct"]
	inst.Typ = item["type"]
	inst.Vol = atoi(item["vol"], 0)
	inst.AZ = item["az"]
	inst.VPC = item["vpc"]
	if tag := item["tag"]; tag != "" {
		inst.Tag = make(cmon.TagMap)
		for i, tv := 1, strings.Split(tag, "\t"); i < len(tv); i += 2 {
			inst.Tag[tv[i-1]] = tv[i]
		}
	} else {
		inst.Tag = nil
	}
	if metric := item["metric"]; metric != "" {
		if inst.Metric == nil {
			inst.Metric = make(cmon.MetricMap)
		}
		for i, mv := 1, strings.Split(metric, "\t"); i < len(mv); i += 2 {
			if v, err := strconv.ParseFloat(mv[i], 32); err == nil {
				inst.Metric[mv[i-1]] = append(inst.Metric[mv[i-1]], float32(v))
			}
		}
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
		if r := work.rates.Lookup(&k); r == nil {
			logE.Printf("no EC2 %v rate found for %v/%v in %v", k.Terms, k.Typ, k.Plat, reg)
		} else if inst.ORate != 0 {
		} else if inst.Spot == "" {
			k.Terms = settings.AWS.SavPlan
			if s := work.rates.Lookup(&k); s == nil {
				inst.ORate = r.Rate
			} else {
				inst.ORate = r.Rate*(1-settings.AWS.SavCov) + s.Rate*settings.AWS.SavCov
			}
		} else {
			inst.ORate = r.Rate * (1 - settings.AWS.SpotDisc)
		}
		inst.Rate = inst.ORate * settings.AWS.UsageAdj
		if inst.Active == nil || inst.Last > inst.Active[len(inst.Active)-1] {
			inst.Active = append(inst.Active, now, now)
		} else {
			inst.Active[len(inst.Active)-1] = now
			dur := uint64(now - inst.Last)
			c := inst.Rate * float32(dur) / 3600
			sum.Current = sum.ByAcct.add(now, int(dur), inst.Acct, dur, c)
			sum.ByRegion.add(now, int(dur), reg, dur, c)
			switch {
			case inst.Spot == "" && inst.Plat == "":
				sum.BySKU.add(now, int(dur), reg+" "+k.Typ, dur, c)
			case inst.Spot != "" && inst.Plat != "":
				sum.BySKU.add(now, int(dur), reg+" sp."+k.Typ+" "+inst.Plat, dur, c)
			case inst.Plat == "":
				sum.BySKU.add(now, int(dur), reg+" sp."+k.Typ, dur, c)
			default:
				sum.BySKU.add(now, int(dur), reg+" "+k.Typ+" "+inst.Plat, dur, c)
			}
		}
	default:
		inst.Rate = 0
	}
	inst.Last = now
}

// ebs.aws model gopher accessors...
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
	vol.GiB = atoi(item["gib"], 0)
	vol.IOPS = atoi(item["iops"], 0)
	vol.MiBps = atoi(item["mibps"], 0)
	vol.AZ = item["az"]
	if vol.Mount = item["mount"]; vol.Mount == "0 attachments" {
		vol.Mount = ""
	}
	if tag := item["tag"]; tag != "" {
		vol.Tag = make(cmon.TagMap)
		for i, tv := 1, strings.Split(tag, "\t"); i < len(tv); i += 2 {
			vol.Tag[tv[i-1]] = tv[i]
		}
	} else {
		vol.Tag = nil
	}
	if metric := item["metric"]; metric != "" {
		if vol.Metric == nil {
			vol.Metric = make(cmon.MetricMap)
		}
		for i, mv := 1, strings.Split(metric, "\t"); i < len(mv); i += 2 {
			if v, err := strconv.ParseFloat(mv[i], 32); err == nil {
				vol.Metric[mv[i-1]] = append(vol.Metric[mv[i-1]], float32(v))
			}
		}
	}

	reg := aws.Region(vol.AZ)
	k, c := aws.EBSRateKey{
		Region: reg,
		Typ:    vol.Typ,
	}, float32(0)
	r := work.rates.Lookup(&k)
	if r == nil {
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
				vol.Rate, c = (r.SZrate*float32(vol.GiB)+r.IOrate*float32(vol.IOPS-3000))*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
			} else {
				vol.Rate, c = r.SZrate*float32(vol.GiB)*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
			}
		default:
			vol.Rate, c = (r.SZrate*float32(vol.GiB)+r.IOrate*float32(vol.IOPS))*settings.AWS.UsageAdj, vol.Rate*float32(dur)/3600
		}
	default:
		vol.Rate = 0
	}
	if c > 0 {
		u := uint64(vol.GiB * dur)
		sum.Current = sum.ByAcct.add(now, dur, vol.Acct, u, c)
		sum.ByRegion.add(now, dur, reg, u, c)
		sum.BySKU.add(now, dur, reg+" "+k.Typ, u, c)
	}
	vol.Last = now
}

// rds.aws model gopher accessors...
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
	db.GiB = atoi(item["gib"], 0)
	db.IOPS = atoi(item["iops"], 0)
	db.Ver = item["ver"]
	db.AZ = item["az"]
	db.VPC = item["vpc"]
	db.Lic = item["lic"]
	if db.MultiAZ = item["multiaz"] == "True"; db.MultiAZ {
		az = 2
	}
	if tag := item["tag"]; tag != "" {
		db.Tag = make(cmon.TagMap)
		for i, tv := 1, strings.Split(tag, "\t"); i < len(tv); i += 2 {
			db.Tag[tv[i-1]] = tv[i]
		}
	} else {
		db.Tag = nil
	}
	if metric := item["metric"]; metric != "" {
		if db.Metric == nil {
			db.Metric = make(cmon.MetricMap)
		}
		for i, mv := 1, strings.Split(metric, "\t"); i < len(mv); i += 2 {
			if v, err := strconv.ParseFloat(mv[i], 32); err == nil {
				db.Metric[mv[i-1]] = append(db.Metric[mv[i-1]], float32(v))
			}
		}
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
	if r == nil {
		logE.Printf("no RDS %v rate found for %v/%v[%v] in %v", k.Terms, k.Typ, k.Plat, db.STyp, reg)
		r = &aws.RateValue{}
	}
	if s == nil {
		s = &aws.EBSRateValue{}
	}
	switch db.State = item["state"]; db.State {
	case "available", "backing-up":
		if db.Active == nil || db.Last > db.Active[len(db.Active)-1] {
			db.Active = append(db.Active, now, now)
		} else {
			db.Active[len(db.Active)-1], u = now, uint64(az*dur)
		}
		db.Rate, c = (r.Rate*float32(az)+s.SZrate*float32(az*db.GiB)+s.IOrate*float32(az*db.IOPS))*settings.AWS.UsageAdj, db.Rate*float32(dur)/3600
	case "stopped", "stopping":
		db.Rate = (s.SZrate*float32(az*db.GiB) + s.IOrate*float32(az*db.IOPS)) * settings.AWS.UsageAdj
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

// snap.aws model gopher accessors...
func snapawsInsert(acc *modAcc, item map[string]string, now int) {
	sum, detail, work, id := acc.m.data[0].(*snapSum), acc.m.data[1].(*snapDetail), acc.m.data[2].(*snapWork), item["id"]
	if item == nil {
		if now > detail.Current {
			detail.Current = now
		}
		for r, pr := 0, 1; pr > 0; r, pr = 0, r { // fix resolvable Vol references
			for _, snap := range detail.Snap {
				if snap.Vol != "" {
				} else if p := detail.Snap[snap.Par]; p != nil && p.VSiz == snap.VSiz && p.Vol != "" {
					snap.Vol = p.Vol
					r++
				}
			}
		}
		return
	} else if id == "" {
		return
	}
	snap, dur := detail.Snap[id], 0
	if snap == nil {
		snap = &snapItem{
			Acct:  item["acct"],
			Typ:   item["type"],
			VSiz:  atoi(item["vsiz"], 0),
			Reg:   item["reg"],
			Since: now,
		}
		if item["vol"] != "vol-ffffffff" {
			snap.Vol = item["vol"]
		}
		if t, err := time.Parse(time.RFC3339, item["since"]); err == nil {
			snap.Since = int(t.Unix())
		}
		detail.Snap[id] = snap
	} else {
		dur = now - snap.Last
	}
	snap.Desc = item["desc"]
	if tag := item["tag"]; tag != "" {
		snap.Tag = make(cmon.TagMap)
		for t, kv := 1, strings.Split(tag, "\t"); t < len(kv); t += 2 {
			snap.Tag[kv[t-1]] = kv[t]
		}
	} else {
		snap.Tag = nil
	}
	if snap.Vol == "" && (snap.Desc != "" || snap.Tag != nil) { // scan metadata to resolve Par/Vol references
		rr := snapRefs.FindAllString(snap.Desc, -1)
		for _, t := range snap.Tag {
			rr = append(rr, snapRefs.FindAllString(t, -1)...)
		}
		for _, r := range rr {
			if strings.HasPrefix(r, "snap-") && r != id {
				if p := detail.Snap[r]; p == nil {
					snap.Par = r
				} else if p.VSiz == snap.VSiz {
					snap.Par, snap.Vol = r, p.Vol
					break
				} else if snap.Par == r {
					snap.Par = ""
				}
			} else if strings.HasPrefix(r, "vol-") && snap.Vol == "" {
				snap.Vol = r
			}
		}
	}

	if snap.Size > 0 {
		k := aws.EBSRateKey{
			Region: snap.Reg,
			Typ:    snap.Typ,
		}
		if r := work.rates.Lookup(&k); r == 0 {
			logE.Printf("no EBS snapshot rate found for %v in %v", k.Typ, snap.Reg)
		} else {
			snap.Rate = r * snap.Size * settings.AWS.UsageAdj
		}
		if u, c := uint64(snap.Size*float32(dur)), snap.Rate*float32(dur)/3600; c > 0 {
			sum.Current = sum.ByAcct.add(now, dur, snap.Acct, u, c)
			sum.ByRegion.add(now, dur, snap.Reg, u, c)
		}
	}
	snap.Last = now
}

// cur.aws model gopher accessors...
func curawsFinalize(acc *modAcc) {
	psum, pdet, work, pg := acc.m.data[0].(*curSum), acc.m.data[1].(*curDetail), acc.m.data[2].(*curWork), lgPage
	var husg []float32
	for mo, wm := range work.idet.Line {
		bt, _ := time.Parse(time.RFC3339, mo[:4]+"-"+mo[4:]+"-01T00:00:00Z")
		bh, eh, pm, tc, tl := int32(bt.Unix())/3600, int32(bt.AddDate(0, 1, 0).Unix()-1)/3600, pdet.Line[mo], 0.0, 0
		for id, line := range wm {
			if pg--; pg <= 0 { // pagination provides cooperative access and avoids write token expiration
				acc.rel()
				pg = lgPage
				acc.reqW()
			}

			if line.Chg <= curItemMin && -curItemMin <= line.Chg || line.Recs == 0 || line.Usg == 0 || len(line.HMap) == 0 {
				delete(wm, id)
				tc += float64(line.Chg)
				tl++
			} else if line.Recs == 1 {
				line.HMap, line.HUsg, line.Recs = nil, nil, line.HMap[0]&baseMask<<foffShift|line.HMap[0]&baseMask+1
			} else if hu := [usgIndex + 2]uint16{}; false {
			} else if fr, to, rw, dev := func() (uint32, uint32, int, float32) {
				var min, max, uv float32
				var f, t uint32
				for _, m := range line.HMap {
					r, ur, b := m>>rangeShift+1, uint16(m>>usgShift&usgMask), m&baseMask
					if ur > usgIndex {
						uv = float32(ur - usgIndex)
					} else {
						uv = line.HUsg[ur]
					}
					if r += b; max == 0 {
						min, max, f, t = uv, uv, b, r
					} else {
						if uv < min {
							min = uv
						} else if uv > max {
							max = uv
						}
						if b < f {
							f = b
						} else if r > t {
							t = r
						}
					}
					for ur++; b < r; b++ { // expand/order map usage references (0 is nil)
						hu[b] = ur
					}
				}
				avg, ut, r := line.Usg/float32(line.Recs), uint16(0), 0
				for _, ur := range hu[f : t+1] { // count optimal (minimum) usage-grouped ranges
					if ur == ut {
						continue
					} else if ut != 0 {
						r++
					}
					ut = ur
				}
				if uv = avg - min; max-avg > uv {
					uv = max - avg
				}
				return f, t, r, uv * line.Chg / line.Usg
			}(); to-fr == line.Recs && dev <= curItemDev && -curItemDev <= dev {
				line.HMap, line.HUsg, line.Recs = nil, nil, (line.Recs-1)<<recsShift|fr<<foffShift|to
			} else if bw := int(to-fr+31+32-hrBMShift) >> 5; bw <= rw+len(line.HUsg) && dev <= curItemDev && -curItemDev <= dev {
				hmap, off := make([]uint32, bw), int32(fr)-32+hrBMShift
				hmap[0] = hrBitmap << hrBMShift // build bit-mapped usage representation
				for _, m := range line.HMap {
					r, b := int32(m>>rangeShift), int32(m&baseMask)-off
					for r += b; b <= r; b++ {
						hmap[b>>5] |= 1 << (31 - b&31)
					}
				}
				line.HMap, line.HUsg, line.Recs = hmap, nil, (line.Recs-1)<<recsShift|fr<<foffShift|to
			} else if int(to-fr) <= rw+len(line.HUsg) {
				husg = make([]float32, int(to-fr)) // build unmapped full usage representation
				for h := 0; h < len(husg); h++ {
					if ur := hu[int(fr)+h]; ur > usgIndex+1 {
						husg[h] = float32(ur - usgIndex - 1)
					} else if ur > 0 {
						husg[h] = line.HUsg[ur-1]
					}
				}
				line.HMap, line.HUsg, line.Recs = nil, husg, (line.Recs-1)<<recsShift|fr<<foffShift|to
			} else {
				line.HMap, line.Recs = make([]uint32, 0, rw), (line.Recs-1)<<recsShift|fr<<foffShift|to
				ht, ut := 0, uint16(0)
				for h, ur := range hu[fr : to+1] { // build range-mapped full usage representation
					if ur == ut {
						continue
					} else if ut != 0 {
						line.HMap = append(line.HMap, uint32(h-ht-1)<<rangeShift|uint32(ut-1)<<usgShift|fr+uint32(ht))
					}
					ht, ut = h, ur
				}
				if len(line.HUsg) != cap(line.HUsg) {
					husg = make([]float32, len(line.HUsg))
					copy(husg, line.HUsg)
					line.HUsg = husg
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
			psum.Hist.update(acc, wm, bh, eh)
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
				work.ihr = int32(t.Unix() / 3600)
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

	var h int32
	if t, err := time.Parse(time.RFC3339, item["hour"]); err == nil {
		if h = int32(t.Unix()/3600) - work.ihr; h > usgIndex {
			h = 0
		}
	}
	u, _ := strconv.ParseFloat(item["usg"], 32)
	c, _ := strconv.ParseFloat(item["chg"], 64)
	line, hr, r, us, ur, ch := work.idetm[id], int32(h+work.ihr), 0, float32(u), uint32(0), float32(c)
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
			Name: item["cmon:Name"],
			Env:  item["cmon:Env"],
			Prod: item["cmon:Prod"],
			Role: item["cmon:Role"],
			Ver:  item["cmon:Ver"],
			Prov: item["cmon:Prov"],
			Oper: item["cmon:Oper"],
			Bill: item["cmon:Bill"],
			Cust: item["cmon:Cust"],
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
	for ; r < len(line.HMap) && line.HMap[r]&baseMask+line.HMap[r]>>rangeShift+1 != uint32(h); r++ {
	}
	if r == len(line.HMap) || line.HMap[r]>>usgShift&usgMask != ur {
		line.HMap = append(line.HMap, ur<<usgShift|uint32(h))
	} else {
		line.HMap[r] += 1 << rangeShift
	}

	line.Usg += us
	line.Chg += ch
	line.Recs++
	// TODO: fix charge/usage clumping for day/mo (non-hourly) reporting
	work.isum.ByAcct.add(hr, line.Acct, c)
	work.isum.ByRegion.add(hr, line.Reg, c)
	work.isum.ByTyp.add(hr, line.Typ, c)
	work.isum.BySvc.add(hr, line.Svc, c)
}

// cdr.asp model gopher accessors...
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
	case typ == "CORE" || strings.HasPrefix(itg, "ACCESS_") || strings.HasPrefix(etg, "ACCESS_") ||
		itg == "MERCEDES" || itg == "USPRODMBZ_ZIPWIRE_TG":
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
			if sp != "customer" {
				e := fmt.Sprintf("[%v/%v] %v", work.sl.Name(lc), sp, err)
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

// general accessor helpers...
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
