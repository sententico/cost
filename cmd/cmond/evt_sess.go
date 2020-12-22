package main

import (
	"math"
	"sort"
	"time"
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

func trigcmonScan(m *model, evt string) {
	switch evt {
	case "cdr.asp":
		for _, metric := range []struct {
			name   string
			thresh float64
			sig    float64
		}{
			{"cdr.asp/term/geo", 60, 6.0},
			{"cdr.asp/term/cust", 40, 6.0},
			{"cdr.asp/term/sp", 320, 6.0},
			{"cdr.asp/term/to", 60, 6.0},
		} {
			if c, err := seriesExtract(metric.name, 24*90, 2, metric.thresh); err != nil {
				logE.Printf("problem accessing %q metric: %v", metric.name, err)
			} else if mm := <-c; mm != nil {
				for na, se := range mm {
					if len(se) < 2 {
					} else if u := se[0] + se[1]*(0.7*float64(3600-(time.Now().Unix()-90)%3600)/3600+0.3); u < metric.thresh {
					} else if ss, mean, sdev := basicStats(se); u > mean+sdev*metric.sig {
						logW.Printf("%q metric signaling fraud: $%.0f usage for %q ($%.0f @95pct)", metric.name, u, na, ss[len(ss)*95/100])
					}
				}
			}
		}
	}
}
