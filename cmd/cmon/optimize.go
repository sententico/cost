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
	minCommit = 0.0
	maxCommit = 10000.0
	initStep  = 100.0
	minStep   = 0.001
)

func compawsOpt(base *cmon.SeriesRet, ho, ivl int) func(int, float64) float64 {
	type skuCell struct {
		od, sp, usage float64
		// TODO: change sp to []float64
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
		// TODO: use alternate call... rates.LookupS() ...to get slice of rates for all terms...
		//		 OD, 1nc,1pc,1ac, 1ns,1ps,1as, 3nc,3pc,3ac, 3ns,3ps,3as
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
		end := hr + 1
		if hr < 0 || hr >= ivl {
			end, hr = ivl, 0
		}
		for ; hr < end; hr++ {
			commit := commit
			for _, cell := range sku[hr] {
				if commit > 0 {
					if disc := cell.sp * cell.usage; commit >= disc {
						cost += disc
						commit -= disc
					} else {
						cost += commit + cell.od*(cell.usage-commit/cell.sp)
						commit = 0
					}
				} else {
					cost += cell.od * cell.usage
				}
			}
			cost += commit
		}
		return // cost for baseline usage hour, hr (over ivl if outside it), based on hourly commit discount
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
	case "optimize ec2.aws/sku/n 1nc", "optimize ec2.aws/sku/n 1pc", "optimize ec2.aws/sku/n 1ac",
		"optimize ec2.aws/sku/n 3nc", "optimize ec2.aws/sku/n 3pc", "optimize ec2.aws/sku/n 3ac":
		cost, min, opt := compawsOpt(&r, ho, ivl), 1e9, 0.0
		for begin, end, step := minCommit, maxCommit, initStep; step > minStep; begin, end, step =
			opt-step, opt+step, step/2 { // converge on optimum commit
			for c := begin; c < end; c += step {
				if t := cost(ivl, c); t < min {
					min, opt = t, c
				}
			}
		}
		low, high := opt-args.bracket/2, opt+args.bracket/2
		fmt.Printf("hour,OD,low=%.0f,opt=%.2f,high=%.0f\n", low, opt, high)
		for h := ivl - 1; h >= 0; h-- {
			fmt.Printf("%v,%.2f,%.2f,%.2f,%.2f\n", -h-ho, cost(h, 0), cost(h, low), cost(h, opt), cost(h, high))
		}
		fmt.Println("\ncommit,interval cost")
		for c := low; c < high; c += args.step {
			fmt.Printf("%.2f,%.2f\n", c, cost(ivl, c))
		}
		fmt.Printf("\n$%.2f %d-hour interval cost at optimum $%.2f commit (%q usage on %q plan)\n\n", min, ivl, opt, args.opMetric, args.plan)

	default:
		fatal(1, "%q subcommand not implemented", command)
	}
}
