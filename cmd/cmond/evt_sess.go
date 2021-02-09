package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	samplePage = 180 // statistics sample page size (1 high/low value eliminated per page)
)

var (
	ec2P = regexp.MustCompile(`\b(Linux( Spot)?|RHEL|Windows( with SQL (SE|EE|Web|EX)| Spot)?|SQL (SE|EE|Web|EX))\b`)
)

func basicStats(s []float64) (ss []float64, mean, sdev float64) {
	if len(s) > 0 {
		ss = append([]float64(nil), s...)
		for sort.Float64s(ss); len(ss) >= samplePage && ss[0] == 0; ss = ss[1:] {
		}
		if t := len(ss) / samplePage; t > 0 {
			ss = ss[t : len(ss)-t]
		}
		for _, v := range ss {
			mean += v
		}
		mean /= float64(len(ss))
		var d float64
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

func trigcmonScan(m *model, event string) {
	switch event {
	case "cdr.asp":
		var alerts []string
		for _, metric := range []struct {
			name   string
			thresh float64
			sig    float64
		}{
			{"cdr.asp/term/geo", 400, 5},
			{"cdr.asp/term/cust", 300, 6},
			{"cdr.asp/term/sp", 1200, 5},
			{"cdr.asp/term/to", 200, 5},
		} {
			if c, err := seriesExtract(metric.name, 24*90, 2, metric.thresh/2.2); err != nil {
				logE.Printf("problem accessing %q metric: %v", metric.name, err)
			} else if sx, now, adj := <-c, time.Now().Unix(), 0.0; sx != nil {
				if adj = 0.2; int32(now/3600) == sx.From {
					adj += float64(3600-now%3600) / 3600
				}
				for k, se := range sx.Series {
					if len(se) == 1 && se[0]*(1+adj) > metric.thresh {
						alerts = append(alerts, fmt.Sprintf(
							"%q metric signaling fraud: new $%.0f usage burst for %q",
							metric.name, se[0], k))
					} else if len(se) < 2 {
					} else if u := se[0] + se[1]*adj; u < metric.thresh {
					} else if len(se) < 4 {
						alerts = append(alerts, fmt.Sprintf(
							"%q metric signaling fraud: new $%.0f hourly usage burst for %q",
							metric.name, u, k))
					} else if ss, mean, sdev := basicStats(se); u > mean+sdev*metric.sig {
						alerts = append(alerts, fmt.Sprintf(
							"%q metric signaling fraud: $%.0f hourly usage for %q "+
								"(normally $%.0f bursting to $%.0f to as much as $%.0f)",
							metric.name, u, k, ss[len(ss)*50/100], ss[len(ss)*95/100], ss[len(ss)-1]))
					}
				}
			}
		}
		for _, a := range alerts {
			logW.Printf(a)
		}
		if err := slackAlerts("#telecom-fraud", alerts); err != nil {
			logE.Printf("Slack alert problem: %v", err)
		}
		// TODO: emailAlerts() docs.aws.amazon.com/ses/latest/DeveloperGuide/examples-send-raw-using-sdk.html
	}
}

func ec2awsFeedback(m *model, event string) {
	defer func() {
		if r := recover(); r != nil {
			logE.Printf("error looping in %q feedback for %q: %v", event, m.name, r)
		}
	}()
	switch event {
	case "cur.aws":
		func() {
			type feedback struct {
				plat        string
				spot        bool
				usage, cost float32
			}
			ec2, det, active := m.newAcc(), m.data[1].(*ec2Detail), make(map[string]*feedback, 4096)
			ec2.reqR()
			defer func() { ec2.rel() }()
			for id, inst := range det.Inst {
				switch inst.State {
				case "running", "pending", "stopped":
					active[id] = nil
				}
			}
			ec2.rel()
			cur, now, pmap := mMod[event].newAcc(), "", map[string]string{
				"RHEL":                 "rhel",
				"Windows":              "windows",
				"Windows Spot":         "windows",
				"Windows with SQL SE":  "sqlserver-se",
				"Windows with SQL EE":  "sqlserver-ee",
				"Windows with SQL Web": "sqlserver-web",
				"Windows with SQL EX":  "sqlserver-ex",
				"SQL SE":               "sqlserver-se",
				"SQL EE":               "sqlserver-ee",
				"SQL Web":              "sqlserver-web",
				"SQL EX":               "sqlserver-ex",
			}
			cur.reqR()
			defer func() { cur.rel() }()
			for mo := range cur.m.data[1].(*curDetail).Line {
				if mo > now {
					now = mo
				}
			}
			for _, item := range cur.m.data[1].(*curDetail).Line[now] {
				if f, found := active[item.RID]; found {
					if f == nil {
						f = &feedback{cost: item.Cost}
						active[item.RID] = f
					} else {
						f.cost += item.Cost
					}
					if p := ec2P.FindString(item.Desc); p != "" {
						if f.plat = pmap[p]; strings.HasSuffix(p, " Spot") {
							f.spot = true
						}
						f.usage += item.Usg
					}
				}
			}
			cur.rel()
			ec2.reqW()
			for id, f := range active {
				if f == nil || f.usage == 0 {
				} else if inst := det.Inst[id]; inst != nil {
					inst.Plat = f.plat
					inst.ORate = f.cost / f.usage
					if f.spot && inst.Spot == "" {
						inst.Spot = "unknown SIR" // TODO: verify this even happens
					}
				}
			}
			ec2.rel()
		}()
	}
}
