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
	minCommit = 1
	maxCommit = 1000
	initStep  = 10
)

func compawsOpt(base *cmon.SeriesRet, ho int) func(int, float64) float64 {
	type skuCell struct {
		od, sp, usage float64
	}
	var cell skuCell
	sku := make([][]skuCell, args.opInterval.to-args.opInterval.from+1)
	var rates aws.Rater
	var rk aws.RateKey
	var od, sp aws.RateValue
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
		od, rk.Terms = rates.Lookup(&rk), args.plan
		sp = rates.Lookup(&rk)
		if cell.od, cell.sp = float64(od.Rate), float64(sp.Rate); cell.od == 0 || cell.sp == 0 {
			fatal(1, "no rates for %v", rk)
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
		return // cost for baseline usage hour, hr, based on hourly commit discount
	}
}

func optimizeCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r cmon.SeriesRet
	var ho int
	if ho = int(int32((time.Now().Unix()-180)/3600) - args.opInterval.from + 1); ho <
		int(args.opInterval.to-args.opInterval.from+2) {
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
		ho -= int(args.opInterval.to - args.opInterval.from + 1)
	}
	switch command {
	case "optimize ec2.aws/sku/n 1nc", "optimize ec2.aws/sku/n 3nc":
		cost := compawsOpt(&r, ho)
		fmt.Printf("cost(0,min)=%v, cost(0,mid)=%v, cost(0,max)=%v\n",
			cost(0, minCommit), cost(0, (minCommit+maxCommit)/2), cost(0, maxCommit))
		// locate optimum commit minimizing cost
		// iterate commit in args.step increments through bracket centered on optimum, calculating cost
		// output undiscounted, optimum, bracket begin/end coordinates over interval
		// output commit/cost coordinates over bracket
	default:
		fatal(1, "%q unsupported", command)
	}
}
