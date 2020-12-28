package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

func basicStats(s []float64) (ss []float64, mean, sdev float64) {
	if t, d := len(s)/48, 0.0; len(s) > 0 {
		ss = append([]float64(nil), s...)
		sort.Float64s(ss)
		for ss, t = ss[t:len(s)-t], len(s)*30/100; len(ss) > t && ss[0] == 0; ss = ss[1:] {
		}
		for _, v := range ss {
			mean += v
		}
		mean /= float64(len(ss))
		for _, v := range ss {
			d = v - mean
			sdev += d * d
		}
		sdev = math.Sqrt(sdev / float64(len(ss)))
	}
	return
}

func slackAlerts(ch string, alerts []string) (err error) {
	if len(alerts) == 0 {
		return
	}
	var weain io.WriteCloser
	var weaout io.ReadCloser
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
		weain.Close()
		weaout.Close()
	}()
	if weain, weaout, err = weaselCmd.new("hook.slack", nil); err != nil {
		return fmt.Errorf("couldn't release weasel: %v", err)
	}
	enc, arg, n := json.NewEncoder(weain), map[string]string{
		"Channel": ch,
	}, 0
	for _, alert := range alerts {
		arg["Text"] = alert
		if err = enc.Encode(&arg); err != nil {
			return fmt.Errorf("error encoding request: %v", err)
		}
	}
	weain.Close()
	if err = json.NewDecoder(weaout).Decode(&n); err != nil {
		return fmt.Errorf("error decoding response: %v", err)
	} else if n != len(alerts) {
		return fmt.Errorf("response code %v", n)
	}
	return
}

func trigcmonScan(m *model, evt string) {
	switch evt {
	case "cdr.asp":
		var alerts []string
		for _, metric := range []struct {
			name   string
			thresh float64
			sig    float64
		}{
			{"cdr.asp/term/geo", 60, 3.5},
			{"cdr.asp/term/cust", 40, 3.5},
			{"cdr.asp/term/sp", 320, 3.5},
			{"cdr.asp/term/to", 60, 3.5},
		} {
			if c, err := seriesExtract(metric.name, 24*90, 2, metric.thresh); err != nil {
				logE.Printf("problem accessing %q metric: %v", metric.name, err)
			} else if mm := <-c; mm != nil {
				for na, se := range mm {
					if len(se) < 2 {
					} else if u := se[0] + se[1]*(0.7*float64(3600-(time.Now().Unix()-90)%3600)/3600+0.3); u < metric.thresh {
					} else if ss, mean, sdev := basicStats(se); u > mean+sdev*metric.sig {
						logW.Printf("%q metric signaling fraud: $%.0f usage for %q ($%.0f @99pct)", metric.name, u, na, ss[len(ss)*99/100])
						alerts = append(alerts, fmt.Sprintf(
							"%q metric signaling fraud: $%.0f hourly usage for %q ($%.0f @median, $%.0f @99pct, $%.0f @max)",
							metric.name, u, na, ss[len(ss)*50/100], ss[len(ss)*99/100], ss[len(ss)-1],
						))
					}
				}
			}
		}
		if err := slackAlerts("#telecom-fraud", alerts); err != nil {
			logE.Printf("Slack alert problem: %v", err)
		}
	}
}
