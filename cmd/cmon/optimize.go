package main

import (
	"fmt"
	"net/rpc"
	"sort"
	"strings"
	"time"

	"github.com/sententico/cost/aws"
	"github.com/sententico/cost/cmon"
)

const (
	minCommit = 1.0
	maxCommit = 1000.0
	initStep  = 10.0
	minStep   = 0.01
)

func compawsOpt(base *cmon.SeriesRet, ho, ivl int) func(int, float64) float64 {
	type skuCell struct {
		od, sp, usage float64
	}
	sku := make([][]skuCell, ivl)
	var cell skuCell
	var rates aws.Rater
	var rk aws.RateKey
	var od, sp *aws.RateValue
	if err := rates.Load(nil, "EC2"); err != nil {
		fatal(1, "cannot load EC2 rates: %v", err)
	}
	for k, ser := range base.Series {
		if ho >= len(ser) || strings.HasPrefix(strings.Split(k, " ")[1], "sp.") {
			continue
		}
		if s := strings.Split(k, " "); len(s) < 3 {
			rk.Region, rk.Typ, rk.Plat, rk.Terms = s[0], s[1], "", "OD"
		} else {
			rk.Region, rk.Typ, rk.Plat, rk.Terms = s[0], s[1], s[2], "OD"
		}
		if od, rk.Terms = rates.Lookup(&rk), args.plan; od == nil {
			fatal(1, "no rates for %v", rk)
		} else if sp, cell.od = rates.Lookup(&rk), float64(od.Rate); sp == nil {
			cell.sp = cell.od // TODO: consider fallback to "s" plan
		} else {
			cell.sp = float64(sp.Rate)
		}
		for h, u := range ser[ho:] {
			if u != 0 {
				cell.usage = u
				sku[h] = append(sku[h], cell)
			}
		}
	}
	for _, cells := range sku {
		sort.Slice(cells, func(i, j int) bool { return cells[i].sp/cells[i].od < cells[j].sp/cells[j].od })
	}
	return func(hr int, commit float64) (cost float64) {
		if commit < 0 {
			commit = 0
		}
		for _, cell := range sku[hr] {
			if commit > 0 {
				if disc := cell.sp * cell.usage; disc <= commit {
					cost += disc
					commit -= disc
				} else {
					cost += cell.od*(cell.usage-commit/cell.sp) + commit
					commit = 0
				}
			} else {
				cost += cell.od * cell.usage
			}
		}
		return cost + commit // cost for baseline usage hour, hr, based on hourly commit discount
	}
}

func optimizeCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r cmon.SeriesRet
	ho, ivl := int(int32((time.Now().Unix()-180)/3600)-args.opInterval.from+1), int(args.opInterval.to-args.opInterval.from+1)
	if ho < ivl+1 {
		fatal(1, "usage baseline interval not available")
	} else if err = client.Call("API.Series", &cmon.SeriesArgs{
		Token:    "placeholder_access_token",
		Metric:   args.opMetric,
		Recent:   ho,
		Truncate: 0,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	} else {
		client.Close()
		ho -= ivl
	}
	switch command {
	case "optimize ec2.aws/sku/n 1nc", "optimize ec2.aws/sku/n 3nc":
		cost := compawsOpt(&r, ho, ivl)
		icost := func(c float64) (t float64) {
			for h := 0; h < ivl; h++ {
				t += cost(h, c)
			}
			return
		}
		min, opt := 1e9, 0.0
		for begin, end, step := minCommit, maxCommit, initStep; step > minStep; begin, end, step =
			opt-step, opt+step, step/2 {
			for c := begin; c < end; c += step {
				if t := icost(c); t < min {
					min, opt = t, c
				}
			}
		}
		fmt.Printf("$%.2f interval cost at optimum $%.2f commit\n", min, opt)
		// output undiscounted, optimum, bracket begin/end coordinates over interval
		// output commit/cost coordinates over bracket
	default:
		fatal(1, "%q unsupported", command)
	}
}
