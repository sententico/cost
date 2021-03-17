package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	samplePage = 300 // statistics sample "page" size (1 high/low anomalous value pair eliminated per page)
)

type (
	alertMetric struct {
		name   string
		thresh float64 // alert threshold amount
		ratio  float64 // minimum ratio to mean
		sig    float64 // minimum sigmas from mean
		alert  func(string, string, ...float64) map[string]string
		filter func(string) string
	}
)

var (
	ec2P = regexp.MustCompile(`\b(Linux( Spot)?|RHEL|Windows( with SQL (SE|EE|Web|EX)| Spot)?|SQL (SE|EE|Web|EX))\b`)
)

func alertWeasel(weasel string, alerts []map[string]string) (err error) {
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
	if weain, weaout, err = weaselCmd.new(weasel, nil); err != nil {
		return fmt.Errorf("couldn't release weasel: %v", err)
	}
	enc, n := json.NewEncoder(weain), 0
	for _, a := range alerts {
		if err = enc.Encode(&a); err != nil {
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

func alertDetail(alert map[string]string, filters []string, rows int) {
	c, err := tableExtract(alert["model"], rows, filters)
	if i := 0; err == nil {
		for row := range c {
			alert[strconv.Itoa(i)] = strings.Join(row, "\",\"")
			i++ // TODO: fix for fields containing double-quotes
		}
	}
}
func alertCURDetail(alert map[string]string, from int32, filters []string, xrows, rows int) {
	c, err := curtabExtract(from, 0, 1, rows, 0.25, filters)
	if i := 0; err == nil {
		// TODO: extract xrows, insert rows, where xrows>>rows and rows are highest cost of xrows
		for row := range c {
			alert[strconv.Itoa(i)] = strings.Join(row, "\",\"")
			i++ // TODO: fix for fields containing double-quotes
		}
	}
}

func basicStats(s []float64) (ss []float64, mean, sdev float64) {
	if len(s) > 0 {
		ss = append([]float64(nil), s...)
		for sort.Float64s(ss); len(ss) > 1 && ss[0] == 0; ss = ss[1:] {
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

func curawsCost(m, k string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 1:
		a["short"] = fmt.Sprintf("%q metric cost alert: new/rare $%.0f hourly usage burst for %q", m, v[0], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual Amazon consumption.  A $%.0f hourly cost burst for %q has occurred.", m, v[0], k)
	case 2:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f)", m, v[0], k, v[1])
		a["long"] = fmt.Sprintf("The %q metric is recording elevated Amazon consumption costing $%.0f per hour for %q.", m, v[0], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour.", v[1])
	case 3:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f with bursts ranging to $%.0f)", m, v[0], k, v[1], v[2])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy Amazon consumption costing $%.0f per hour for %q.", m, v[0], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f.", v[1], v[2])
	case 4:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f bursting to $%.0f to as much as $%.0f)", m, v[0], k, v[1], v[2], v[3])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy Amazon consumption costing $%.0f per hour for %q.", m, v[0], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f or occasionally to as much as $%.0f.", v[1], v[2], v[3])
	default:
		return nil
	}
	return
}
func curCost() (alerts []map[string]string) {
	for _, metric := range []alertMetric{
		{"cur.aws/acct", 4, 1.2, 3, curawsCost, func(k string) string { return `acct~^` + k }},
		{"cur.aws/region", 8, 1.2, 3, curawsCost, func(k string) string { return `region=` + k }},
		{"cur.aws/typ", 8, 1.2, 3, curawsCost, func(k string) string { return `typ=` + k }},
		{"cur.aws/svc", 2, 1.2, 3, curawsCost, func(k string) string { return `svc=` + k }},
	} {
		if c, err := seriesExtract(metric.name, 24*100, 12, metric.thresh); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx := <-c; sx != nil {
			for k, se := range sx.Series {
				var a map[string]string
				if len(se) <= 12 {
					if _, u, _ := basicStats(se); u > metric.thresh {
						a = metric.alert(metric.name, k, u)
					}
				} else if _, u, _ := basicStats(se[:12]); u <= metric.thresh {
				} else if ss, mean, sdev := basicStats(se[12:]); u > mean*metric.ratio && u > mean+sdev*metric.sig {
					switch med, high, max := ss[len(ss)*50/100], ss[len(ss)*95/100], ss[len(ss)-1]; {
					case high-med < 1 && max-high < 1:
						a = metric.alert(metric.name, k, u, max)
					case high-med < 1 || max-high < 1:
						a = metric.alert(metric.name, k, u, med, max)
					default:
						a = metric.alert(metric.name, k, u, med, high, max)
					}
				}
				if a != nil {
					a["profile"] = "cost"
					a["cols"] = "Invoice Item,Hour,AWS Account,Type,Service,Usage Type,Operation,Region,Resource ID,Item Description,Name,env,dc,product,app,cust,team,version,~,Usage,Billed"
					alertCURDetail(a, -12, []string{
						metric.filter(k),
					}, 2000, 240)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func cdrtermFraud(m, k string, v ...float64) (a map[string]string) {
	a = make(map[string]string, 64)
	switch a["model"] = "cdr.asp/term"; len(v) {
	case 1:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: new/rare $%.0f usage burst for %q", m, v[0], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual outbound call activity. A billable $%.0f usage burst for %q is occurring.", m, v[0], k)
	case 2:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: new/rare $%.0f hourly usage burst for %q", m, v[1], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual outbound call activity. A billable $%.0f hourly usage burst for %q is occurring.", m, v[1], k)
	case 3:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f)", m, v[1], k, v[2])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy outbound call activity amounting to $%.0f of hourly billable usage for %q.", m, v[1], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour.", v[2])
	case 4:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f with bursts ranging to $%.0f)", m, v[1], k, v[2], v[3])
		a["long"] = fmt.Sprintf("The %q metric is recording especially heavy outbound call activity amounting to $%.0f of hourly billable usage for %q.", m, v[1], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f.", v[2], v[3])
	case 5:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f bursting to $%.0f to as much as $%.0f)", m, v[1], k, v[2], v[3], v[4])
		a["long"] = fmt.Sprintf("The %q metric is recording especially heavy outbound call activity amounting to $%.0f of hourly billable usage for %q.", m, v[1], k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f or occasionally to as much as $%.0f.", v[2], v[3], v[4])
	default:
		return nil
	}
	return
}
func cdrtermcustFraud(m, k string, v ...float64) (a map[string]string) {
	if a = make(map[string]string, 64); k == "" {
		k = "any/any"
	}
	switch a["cust"], a["model"] = strings.Split(k, "/")[0], "cdr.asp/term"; len(v) {
	case 1:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: new/rare $%.0f usage burst", k, settings.Alerts.Customers[a["cust"]]["name"], v[0])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing new or unusual outbound call activity. A billable $%.0f usage burst is occurring.", k, settings.Alerts.Customers[a["cust"]]["name"], v[0])
		a["c.long"] = fmt.Sprintf("We're noticing a burst of new or unusual outbound call spending estimated at $%.0f on your %s account (%s).", v[0], settings.Alerts.Customers[a["cust"]]["name"], k)
	case 2:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: new/rare $%.0f hourly usage burst", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing new or unusual outbound call activity. A billable $%.0f hourly usage burst is occurring.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["c.long"] = fmt.Sprintf("We're noticing a burst of new or unusual outbound call spending at an estimated hourly rate of $%.0f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
	case 3:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing heavy outbound call activity amounting to $%.0f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour.", v[2])
		a["c.long"] = fmt.Sprintf("We're noticing elevated outbound call spending at an estimated hourly rate of $%.0f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.0f per hour.", v[2])
	case 4:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f with bursts ranging to $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2], v[3])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing especially heavy outbound call activity amounting to $%.0f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f.", v[2], v[3])
		a["c.long"] = fmt.Sprintf("We're noticing especially elevated outbound call spending at an estimated hourly rate of $%.0f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.0f per hour, with bursts ranging to $%.0f.", v[2], v[3])
	case 5:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f bursting to $%.0f to as much as $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2], v[3], v[4])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing especially heavy outbound call activity amounting to $%.0f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.0f or occasionally to as much as $%.0f.", v[2], v[3], v[4])
		a["c.long"] = fmt.Sprintf("We're noticing especially elevated outbound call spending at an estimated hourly rate of $%.0f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.0f per hour, with bursts ranging to $%.0f or occasionally to as much as $%.0f.", v[2], v[3], v[4])
	default:
		return nil
	}
	return
}
func cdrFraud() (alerts []map[string]string) {
	for _, metric := range []alertMetric{
		{"cdr.asp/term/geo", 600, 1.2, 5, cdrtermFraud, func(k string) string { return `to~ ` + k + `$` }},
		{"cdr.asp/term/cust", 400, 1.2, 6, cdrtermcustFraud, func(k string) string { return `cust=` + k }},
		{"cdr.asp/term/sp", 1200, 1.2, 5, cdrtermFraud, func(k string) string { return `sp=` + k }},
		{"cdr.asp/term/to", 200, 1.2, 5, cdrtermFraud, func(k string) string { return `to~^\` + k[:strings.LastIndexByte(k, ' ')+1] }},
	} {
		if c, err := seriesExtract(metric.name, 24*100, 2, metric.thresh/2.2); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx, now, adj := <-c, time.Now().Unix(), 0.0; sx != nil {
			if adj = 0.2; int32(now/3600) == sx.From {
				adj += float64(3600-now%3600) / 3600
			}
			for k, se := range sx.Series {
				var a map[string]string
				if len(se) == 1 && se[0]*(1+adj) > metric.thresh {
					a = metric.alert(metric.name, k, se[0])
				} else if len(se) < 2 {
				} else if u := se[0] + se[1]*adj; u <= metric.thresh {
				} else if ss, mean, sdev := basicStats(se[2:]); len(ss) == 0 {
					a = metric.alert(metric.name, k, se[0], u)
				} else if u > mean*metric.ratio && u > mean+sdev*metric.sig {
					switch med, high, max := ss[len(ss)*50/100], ss[len(ss)*95/100], ss[len(ss)-1]; {
					case high-med < 1 && max-high < 1:
						a = metric.alert(metric.name, k, se[0], u, max)
					case high-med < 1 || max-high < 1:
						a = metric.alert(metric.name, k, se[0], u, med, max)
					default:
						a = metric.alert(metric.name, k, se[0], u, med, high, max)
					}
				}
				if a != nil {
					a["profile"] = "telecom fraud"
					a["cols"] = "CDR,Loc,To,From,Prov,Cust/App,Start,Min,Tries,Billable,Margin"
					a["c.cols"] = "CDR,Loc,To,From,~,Cust/App,Start,Min,Tries,Billable,~"
					alertDetail(a, []string{
						metric.filter(k),
						fmt.Sprintf(`start>%s`, time.Unix(now-60*90, 0).UTC().Format(time.RFC3339)),
					}, 48)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func trigcmonScan(m *model, event string) {
	var alerts []map[string]string
	switch event {
	case "ec2.aws", "ebs.aws", "rds.aws":
	case "cur.aws":
		alerts = append(alerts, curCost()...)
	case "cdr.asp":
		alerts = append(alerts, cdrFraud()...)
	}
	if len(alerts) > 0 {
		for _, a := range alerts {
			logW.Print(a["short"])
		}
		for _, weasel := range []struct {
			cmd, name string
		}{
			{"hook.slack", "Slack"},
			{"ses.aws", "SES"},
		} {
			if err := alertWeasel(weasel.cmd, alerts); err != nil {
				logE.Printf("%s alert problem: %v", weasel.name, err)
			}
		}
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
