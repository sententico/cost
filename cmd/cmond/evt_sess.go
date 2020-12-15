package main

import (
	"math"
	"sort"
)

func basicStats(s []float64) (ss []float64, mean, sdev float64) {
	if t, d := len(s)/48, 0.0; len(s) > 0 {
		ss = append([]float64(nil), s...)
		sort.Float64s(ss)
		sub := ss[t : len(ss)-t]
		for _, v := range sub {
			mean += v
		}
		mean /= float64(len(sub))
		for _, v := range sub {
			d = v - mean
			sdev += d * d
		}
		sdev = math.Sqrt(sdev / float64(len(sub)))
	}
	return
}

func trigcmonScan(n string, evt string) {
	switch evt {
	case "cdr.asp":
		for _, metric := range []string{
			"cdr.asp/term/geo",
			"cdr.asp/term/cust",
			"cdr.asp/term/sp",
		} {
			if c, err := accSeries(metric, 24*90, 4, 60); err != nil {
				logE.Printf("problem accessing %q metric: %v", metric, err)
			} else if m := <-c; m != nil {
				for n, se := range m {
					if len(se) < 2 {
					} else if r := se[0] + se[1]*0.5; r < 30 {
					} else if ss, mean, sdev := basicStats(se); r > mean+sdev*3 {
						logW.Printf("%q metric signaling fraud: $%.2f usage for %q ($%.0f @95pct)", metric, r, n, ss[len(ss)*95/100])
					}
				}
			}
		}
	}
}
