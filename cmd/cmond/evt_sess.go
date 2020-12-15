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
		if c, err := accSeries("cdr.asp/term/geo", 24*90, 4, 0.0); err != nil {
			logE.Printf("problem accessing %q series: %v", evt, err)
		} else if m := <-c; m != nil {
			if se := m["afr"]; len(se) > 0 {
				ss, mean, sdev := basicStats(se)
				logI.Printf("%q afr series: mean=$%.2f, sdev=$%.2f, se=%v, len=%d", evt, mean, sdev, se[:4], len(ss))
			}
			if se := m["natf"]; len(se) > 0 {
				ss, mean, sdev := basicStats(se)
				logI.Printf("%q natf series: mean=$%.2f, sdev=$%.2f, se=%v, len=%d", evt, mean, sdev, se[:4], len(ss))
			}
		}
	}
}
