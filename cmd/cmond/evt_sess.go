package main

import (
	"crypto/sha256"
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
		label  string
		thresh float64 // alert threshold amount
		ratio  float64 // minimum ratio to mean (and alternative uses)
		sig    float64 // minimum sigmas from mean
		reset  float32 // hours to reset
		alert  func(string, string, string, ...float64) map[string]string
		filter func(string) []string
	}
)

var (
	ec2P = regexp.MustCompile(`\b(Linux( Spot)?|RHEL|Windows( with SQL (SE|EE|Web|EX)| Spot)?|SQL (SE|EE|Web|EX))\b`)

	ec2Metrics = []alertMetric{
		{"ec2.aws/acct", "AWS account", 4, 0.05, 5, 2, ec2Usage, func(k string) []string { return []string{`acct~^` + k} }},
		{"ec2.aws/region", "service location", 12, 0.05, 5, 2, ec2Usage, func(k string) []string { return []string{`az~^` + k} }},
		{"ec2.aws/sku", "instance SKU", 6, 0.05, 5, 2, ec2Usage, func(k string) []string {
			if s, p := strings.Split(k, " "), ""; len(s) > 1 {
				if len(s) == 3 {
					p = s[2]
				}
				if strings.HasPrefix(s[1], "sp.") {
					return []string{
						`az[` + s[0],
						`typ=` + s[1][3:],
						`spot!`,
						`plat=` + p,
					}
				}
				return []string{
					`az[` + s[0],
					`typ=` + s[1],
					`spot=`,
					`plat=` + p,
				}
			}
			return nil
		}},
	}
	ebsMetrics = []alertMetric{
		{"ebs.aws/acct", "AWS account", 6, 0.05, 5, 2, ebsUsage, func(k string) []string { return []string{`acct~^` + k} }},
		{"ebs.aws/region", "service location", 12, 0.05, 5, 2, ebsUsage, func(k string) []string { return []string{`az~^` + k} }},
		{"ebs.aws/sku", "storage SKU", 6, 0.05, 5, 2, ebsUsage, func(k string) []string {
			if s := strings.Split(k, " "); len(s) == 2 {
				return []string{
					`az[` + s[0],
					`typ=` + s[1],
				}
			}
			return nil
		}},
	}
	rdsMetrics = []alertMetric{
		{"rds.aws/acct", "AWS account", 4, 0.05, 5, 2, rdsUsage, func(k string) []string { return []string{`acct~^` + k} }},
		{"rds.aws/region", "service location", 12, 0.05, 5, 2, rdsUsage, func(k string) []string { return []string{`az~^` + k} }},
		{"rds.aws/sku", "database SKU", 2, 0.05, 5, 2, rdsUsage, func(k string) []string {
			if s := strings.Split(k, " "); len(s) == 3 {
				return []string{
					`az[` + s[0],
					`typ=` + s[1],
					`eng=` + s[2],
				}
			}
			return nil
		}},
	}
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

func coreStats(s []float64, zeroes bool, page int) (ss []float64, mean, sdev float64) {
	if len(s) > 0 {
		ss = append(make([]float64, 0, len(s)), s...)
		if sort.Float64s(ss); !zeroes {
			var z, p int
			for ; z < len(ss) && ss[z] < 0; z++ {
			}
			for p = z; p < len(ss) && ss[p] == 0; p++ {
			}
			if l := len(ss) - p + z; l == 0 {
				return nil, 0, 0
			} else if z == 0 {
				ss = ss[p:]
			} else {
				copy(ss[z:], ss[p:])
				ss = ss[:l]
			}
		}
		if page > 2 {
			if t := len(ss) / page; t > 0 {
				ss = ss[t : len(ss)-t]
			}
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

func alertEnabled(a map[string]string, metric alertMetric, k, p string) bool {
	if a == nil {
		return false
	} else if acc := mMod["evt.cmon"].newAcc(); acc == nil {
		return true // TODO: may consider false as code evolves
	} else {
		acc.reqW()
		defer acc.rel()

		evt, id := acc.m.data[0].(*evtModel), strings.Join([]string{metric.name, k, p}, "~")
		rst, _ := time.Parse(time.RFC3339, evt.Alert[id]["reset"])
		if time.Until(rst) > 0 {
			return false
		}
		a["reset"] = time.Now().Add(time.Second * time.Duration(metric.reset*3600)).Format(time.RFC3339)
		a["profile"] = p
		h := sha256.New()
		h.Write([]byte(id + "sa!tyWease1s"))
		a["hash"] = fmt.Sprintf("%x", h.Sum(nil))
		copy := make(map[string]string, len(a))
		for ak, av := range a {
			copy[ak] = av
		}
		evt.Alert[id] = copy
		return true
	}
}

func alertDetail(alert map[string]string, filters []string, rows int) {
	c, err := tableExtract(alert["model"], rows, filters)
	if i := 0; err == nil {
		for row := range c {
			alert[strconv.Itoa(i)] = strings.Join(row, "\",\"")
			// TODO: fix for fields containing double-quotes
			i++
		}
	}
}
func alertCURDetail(alert map[string]string, from int32, filters []string, xrows, rows int) {
	if c, err := curtabExtract(from, 0, 1, xrows, 0.01, filters); err == nil {
		m := make(map[string][]string, xrows/2)
		for row := range c {
			m[row[0]] = row
		}
		type rowVal struct {
			rid string
			val float64
		}
		rv, v := make([]rowVal, 0, len(m)), 0.0
		for i, row := range m {
			v, _ = strconv.ParseFloat(row[len(row)-1], 64)
			rv = append(rv, rowVal{i, v})
		}
		sort.Slice(rv, func(p, q int) bool {
			return rv[p].val >= rv[q].val
		})
		for i := 0; i < len(rv) && i < rows; i++ {
			alert[strconv.Itoa(i)] = strings.Join(m[rv[i].rid], "\",\"")
			// TODO: fix for fields containing double-quotes
		}
	}
}

func ec2Usage(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 2:
		a["short"] = fmt.Sprintf("EC2 instance usage alert: $%.0f hourly usage deviating from $%.0f baseline for %q", v[0], v[1], k)
		a["long"] = fmt.Sprintf("EC2 instance usage is experiencing deviation from its norm. The hourly usage for %s %q, now at $%.2f, is diverging from its $%.2f baseline.", l, k, v[0], v[1])
	default:
		return nil
	}
	a["model"], a["cols"] = "ec2.aws", "Inst,Acct,Type,Plat,Vol,AZ,AMI,Spot,Name,env,dc,product,app,cust,team,version,State,Since,Active%,ORate,Rate"
	return
}
func ebsUsage(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 2:
		a["short"] = fmt.Sprintf("EBS storage usage alert: $%.0f hourly usage deviating from $%.0f baseline for %q", v[0], v[1], k)
		a["long"] = fmt.Sprintf("EBS storage usage is experiencing deviation from its norm. The hourly usage for %s %q, now at $%.2f, is diverging from its $%.2f baseline.", l, k, v[0], v[1])
	default:
		return nil
	}
	a["model"], a["cols"] = "ebs.aws", "Vol,Acct,Type,Size,IOPS,AZ,Mount,Name,env,dc,product,app,cust,team,version,State,Since,Active%,Rate"
	return
}
func rdsUsage(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 2:
		a["short"] = fmt.Sprintf("RDS DB usage alert: $%.0f hourly usage deviating from $%.0f baseline for %q", v[0], v[1], k)
		a["long"] = fmt.Sprintf("RDS database usage is experiencing deviation from its norm. The hourly usage for %s %q, now at $%.2f, is diverging from its $%.2f baseline.", l, k, v[0], v[1])
	default:
		return nil
	}
	a["model"], a["cols"] = "rds.aws", "DB,Acct,Type,Sto,Size,IOPS,Engine,EngVer,Lic,AZ,Name,env,dc,product,app,cust,team,version,State,Since,Active%,Rate"
	return
}
func awsRising(metrics []alertMetric) (alerts []map[string]string) {
	const recent = 20
	for _, metric := range metrics {
		if c, err := seriesExtract(metric.name, 0, recent, func(s []float64) bool {
			if len(s) < recent {
				return true
			} else if _, mean, sdev := coreStats(s[2:], true, 0); mean == 0 || sdev/mean > metric.ratio || s[1] < metric.thresh {
				return true
			} else {
				return s[1] < mean+mean*metric.ratio+sdev*metric.sig+2
			}
		}); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx := <-c; sx != nil {
			for k, se := range sx.Series {
				_, rm, _ := coreStats(se[2:], true, 0)
				if a := metric.alert(metric.name, k, metric.label, se[1], rm); alertEnabled(a, metric, k, "rising usage") {
					alertDetail(a, append(metric.filter(k),
						`act>1.5`,
					), 240)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}
func awsFalling(metrics []alertMetric) (alerts []map[string]string) {
	const recent = 20
	for _, metric := range metrics {
		if c, err := seriesExtract(metric.name, 0, recent, func(s []float64) bool {
			if len(s) < recent {
				return true
			} else if _, mean, sdev := coreStats(s[2:], true, 0); mean == 0 || sdev/mean > metric.ratio || mean < metric.thresh {
				return true
			} else {
				return s[1] > mean-mean*metric.ratio-sdev*metric.sig-2
			}
		}); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx := <-c; sx != nil {
			for k, se := range sx.Series {
				_, rm, _ := coreStats(se[2:], true, 0)
				if a := metric.alert(metric.name, k, metric.label, se[1], rm); alertEnabled(a, metric, k, "falling usage") {
					alertDetail(a, append(metric.filter(k),
						`act<1.5`,
					), 240)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func curawsCost(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 1:
		a["short"] = fmt.Sprintf("%q metric cost alert: new/rare $%.0f hourly usage burst for %q", m, v[0], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual AWS consumption.  A $%.2f hourly cost burst for %s %q has occurred.", m, v[0], l, k)
	case 2:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f)", m, v[0], k, v[1])
		a["long"] = fmt.Sprintf("The %q metric is recording elevated AWS consumption costing $%.2f per hour for %s %q.", m, v[0], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour.", v[1])
	case 3:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f with bursts ranging to $%.0f)", m, v[0], k, v[1], v[2])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy AWS consumption costing $%.2f per hour for %s %q.", m, v[0], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour, with bursts ranging to $%.2f.", v[1], v[2])
	case 4:
		a["short"] = fmt.Sprintf("%q metric cost alert: $%.0f hourly usage for %q (normally $%.0f bursting to $%.0f to as much as $%.0f)", m, v[0], k, v[1], v[2], v[3])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy AWS consumption costing $%.2f per hour for %s %q.", m, v[0], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour, with bursts ranging to $%.2f or occasionally to as much as $%.2f.", v[1], v[2], v[3])
	default:
		return nil
	}
	return
}
func curCost() (alerts []map[string]string) {
	const recent = 12
	for _, metric := range []alertMetric{
		{"cur.aws/acct", "AWS account", 6, 1.1, 2.5, 24, curawsCost, func(k string) []string { return []string{`acct[` + k} }},
		{"cur.aws/region", "service location", 12, 1.1, 2.5, 24, curawsCost, func(k string) []string { return []string{`region=` + k} }},
		{"cur.aws/typ", "billing type", 12, 1.1, 2.5, 24, curawsCost, func(k string) []string { return []string{`typ=` + k} }},
		{"cur.aws/svc", "service", 2, 1.1, 2.5, 24, curawsCost, func(k string) []string { return []string{`svc=` + k} }},
	} {
		if c, err := seriesExtract(metric.name, 24*100, recent, metric.thresh); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx := <-c; sx != nil {
			for k, se := range sx.Series {
				var a map[string]string
				if _, u, _ := coreStats(se[:recent], true, 0); len(se) <= recent {
					a = metric.alert(metric.name, k, metric.label, u*recent/float64(len(se)))
				} else if ss, mean, sdev := coreStats(se[recent:], true, 0); u > mean*metric.ratio && u > mean+sdev*metric.sig {
					switch med, high, max := ss[len(ss)*50/100], ss[len(ss)*95/100], ss[len(ss)-1]; {
					case high-med < 1 && max-high < 1:
						a = metric.alert(metric.name, k, metric.label, u, max)
					case high-med < 1 || max-high < 1:
						a = metric.alert(metric.name, k, metric.label, u, med, max)
					default:
						a = metric.alert(metric.name, k, metric.label, u, med, high, max)
					}
				}
				if alertEnabled(a, metric, k, "cloud cost") {
					a["cols"] = "Invoice Item,Hour,AWS Account,Type,Service,Usage Type,Operation,Region,Resource ID,Item Description,Name,env,dc,product,app,cust,team,version,~,Usage,Billed"
					alertCURDetail(a, -recent, metric.filter(k), 24000, 240)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func cdrtermFraud(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 64); len(v) {
	case 1:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: new/rare $%.0f usage burst for %q", m, v[0], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual outbound call activity. A billable $%.2f usage burst for %s %q is occurring.", m, v[0], l, k)
	case 2:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: new/rare $%.0f hourly usage burst for %q", m, v[1], k)
		a["long"] = fmt.Sprintf("The %q metric is recording new or unusual outbound call activity. A billable $%.2f hourly usage burst for %s %q is occurring.", m, v[1], l, k)
	case 3:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f)", m, v[1], k, v[2])
		a["long"] = fmt.Sprintf("The %q metric is recording heavy outbound call activity amounting to $%.2f of hourly billable usage for %s %q.", m, v[1], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour.", v[2])
	case 4:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f with bursts ranging to $%.0f)", m, v[1], k, v[2], v[3])
		a["long"] = fmt.Sprintf("The %q metric is recording especially heavy outbound call activity amounting to $%.2f of hourly billable usage for %s %q.", m, v[1], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour, with bursts ranging to $%.2f.", v[2], v[3])
	case 5:
		a["short"] = fmt.Sprintf("%q metric signaling fraud: $%.0f hourly usage for %q (normally $%.0f bursting to $%.0f to as much as $%.0f)", m, v[1], k, v[2], v[3], v[4])
		a["long"] = fmt.Sprintf("The %q metric is recording especially heavy outbound call activity amounting to $%.2f of hourly billable usage for %s %q.", m, v[1], l, k)
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour, with bursts ranging to $%.2f or occasionally to as much as $%.2f.", v[2], v[3], v[4])
	default:
		return nil
	}
	a["model"] = "cdr.asp/term"
	return
}
func cdrtermcustFraud(m, k, l string, v ...float64) (a map[string]string) {
	if a = make(map[string]string, 64); k == "" {
		k = "any/any"
	}
	switch a["cust"], a["model"] = strings.Split(k, "/")[0], "cdr.asp/term"; len(v) {
	case 1:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: new/rare $%.0f usage burst", k, settings.Alerts.Customers[a["cust"]]["name"], v[0])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing new or unusual outbound call activity. A billable $%.2f usage burst is occurring.", k, settings.Alerts.Customers[a["cust"]]["name"], v[0])
		a["c.long"] = fmt.Sprintf("We're noticing a burst of new or unusual outbound call spending estimated at $%.2f on your %s account (%s).", v[0], settings.Alerts.Customers[a["cust"]]["name"], k)
	case 2:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: new/rare $%.0f hourly usage burst", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing new or unusual outbound call activity. A billable $%.2f hourly usage burst is occurring.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["c.long"] = fmt.Sprintf("We're noticing a burst of new or unusual outbound call spending at an estimated hourly rate of $%.2f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
	case 3:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing heavy outbound call activity amounting to $%.2f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour.", v[2])
		a["c.long"] = fmt.Sprintf("We're noticing elevated outbound call spending at an estimated hourly rate of $%.2f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.2f per hour.", v[2])
	case 4:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f with bursts ranging to $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2], v[3])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing especially heavy outbound call activity amounting to $%.2f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.0f per hour, with bursts ranging to $%.2f.", v[2], v[3])
		a["c.long"] = fmt.Sprintf("We're noticing especially elevated outbound call spending at an estimated hourly rate of $%.2f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.2f per hour, with bursts ranging to $%.2f.", v[2], v[3])
	case 5:
		a["short"] = fmt.Sprintf("Account %q (%s) signaling fraud: $%.0f hourly usage (normally $%.0f bursting to $%.0f to as much as $%.0f)", k, settings.Alerts.Customers[a["cust"]]["name"], v[1], v[2], v[3], v[4])
		a["long"] = fmt.Sprintf("Account/application %q (%s) is seeing especially heavy outbound call activity amounting to $%.2f of hourly billable usage.", k, settings.Alerts.Customers[a["cust"]]["name"], v[1])
		a["long"] += fmt.Sprintf(" For comparison, typical usage runs about $%.2f per hour, with bursts ranging to $%.2f or occasionally to as much as $%.2f.", v[2], v[3], v[4])
		a["c.long"] = fmt.Sprintf("We're noticing especially elevated outbound call spending at an estimated hourly rate of $%.2f on your %s account (%s).", v[1], settings.Alerts.Customers[a["cust"]]["name"], k)
		a["c.long"] += fmt.Sprintf(" For comparison, your typical spending runs about $%.2f per hour, with bursts ranging to $%.2f or occasionally to as much as $%.2f.", v[2], v[3], v[4])
	default:
		return nil
	}
	return
}
func cdrFraud() (alerts []map[string]string) {
	for _, metric := range []alertMetric{
		{"cdr.asp/term/geo", "geographic zone", 600, 1.2, 5, 0.5, cdrtermFraud, func(k string) []string { return []string{`to] ` + k} }},
		{"cdr.asp/term/cust", "account/app", 400, 1.2, 5.5, 0.5, cdrtermcustFraud, func(k string) []string { return []string{`cust=` + k} }},
		{"cdr.asp/term/sp", "service provider", 1200, 1.2, 5, 0.5, cdrtermFraud, func(k string) []string { return []string{`sp=` + k} }},
		{"cdr.asp/term/to", "termination prefix", 200, 1.2, 5, 0.5, cdrtermFraud, func(k string) []string { return []string{`to[` + k[:strings.LastIndexByte(k, ' ')+1]} }},
	} {
		if c, err := seriesExtract(metric.name, 24*100, 2, metric.thresh/1.2/2); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx, now, hr := <-c, time.Now().Unix(), 0.0; sx != nil {
			if int32(now/3600) == sx.From {
				hr = float64(3599-now%3600) / 3600
			}
			for k, se := range sx.Series {
				var a map[string]string
				if len(se) == 1 && se[0]/(1-hr)*1.3 > metric.thresh {
					a = metric.alert(metric.name, k, metric.label, se[0])
				} else if len(se) < 2 {
				} else if u := (se[0] + se[1]*hr) * 1.2; u <= metric.thresh {
				} else if ss, mean, sdev := coreStats(se[2:], false, samplePage); len(ss) == 0 {
					a = metric.alert(metric.name, k, metric.label, se[0], u)
				} else if u > mean*metric.ratio && u > mean+sdev*metric.sig {
					switch med, high, max := ss[len(ss)*50/100], ss[len(ss)*95/100], ss[len(ss)-1]; {
					case high-med < 1 && max-high < 1:
						a = metric.alert(metric.name, k, metric.label, se[0], u, max)
					case high-med < 1 || max-high < 1:
						a = metric.alert(metric.name, k, metric.label, se[0], u, med, max)
					default:
						a = metric.alert(metric.name, k, metric.label, se[0], u, med, high, max)
					}
				}
				if alertEnabled(a, metric, k, "telecom fraud") {
					a["cols"] = "CDR,Loc,To,From,Prov,Cust/App,Start,Min,Tries,Billable,Margin"
					a["c.cols"] = "CDR,Loc,To,From,~,Cust/App,Start,Min,Tries,Billable,~"
					alertDetail(a, append(metric.filter(k),
						fmt.Sprintf(`start>%s`, time.Unix(now-60*90, 0).UTC().Format(time.RFC3339)),
					), 48)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func cdrtermMargin(m, k, l string, v ...float64) (a map[string]string) {
	switch a = make(map[string]string, 256); len(v) {
	case 1:
		a["short"] = fmt.Sprintf("%q metric margin alert: %.0f%% margin being observed for %q", m, v[0]*100, k)
		a["long"] = fmt.Sprintf("The %q metric is recording sustained low margin performance for outbound calls. The margin for %s %q is currently being observed at %.1f%%.", m, l, k, v[0]*100)
	default:
		return nil
	}
	a["model"] = "cdr.asp/term"
	return
}
func cdrMargin() (alerts []map[string]string) {
	const recent = 40
	for _, metric := range []alertMetric{
		{"cdr.asp/term/geo/p", "geographic zone", 0, 0, 0, 24 * 7, cdrtermMargin, func(k string) []string { return []string{`to] ` + k} }},
		{"cdr.asp/term/cust/p", "account/app", 0.06, 0, 0, 24 * 7, cdrtermMargin, func(k string) []string { return []string{`cust=` + k} }},
		{"cdr.asp/term/sp/p", "service provider", -0.06, 0, 0, 24 * 7, cdrtermMargin, func(k string) []string { return []string{`sp=` + k} }},
	} {
		if c, err := seriesExtract(metric.name, 0, recent, func(s []float64) bool {
			n, sum := 0, 0.0
			for _, v := range s {
				if v != 0 {
					n++
					sum += v
				}
			}
			return n < 8 || sum/float64(n) > metric.thresh
		}); err != nil {
			logE.Printf("problem accessing %q metric: %v", metric.name, err)
		} else if sx, now := <-c, time.Now().Unix(); sx != nil {
			for k, se := range sx.Series {
				_, p, _ := coreStats(se, false, 0)
				if a := metric.alert(metric.name, k, metric.label, p); alertEnabled(a, metric, k, "telecom margin") {
					a["cols"] = "CDR,Loc,To,From,Prov,Cust/App,Start,Min,Tries,Billable,Margin"
					alertDetail(a, append(metric.filter(k),
						// cannot filter %margin with: fmt.Sprintf(`margin<%g`, metric.thresh),
						fmt.Sprintf(`start>%s`, time.Unix(now-3600*recent, 0).UTC().Format(time.RFC3339)),
					), 240)
					alerts = append(alerts, a)
				}
			}
		}
	}
	return
}

func evtcmonHandler(m *model, event *modEvt) {
	var alerts []map[string]string
	switch event.name {
	case "ec2.aws":
		alerts = append(alerts, awsRising(ec2Metrics)...)
		alerts = append(alerts, awsFalling(ec2Metrics)...)
	case "ebs.aws":
		alerts = append(alerts, awsRising(ebsMetrics)...)
		alerts = append(alerts, awsFalling(ebsMetrics)...)
	case "rds.aws":
		alerts = append(alerts, awsRising(rdsMetrics)...)
		alerts = append(alerts, awsFalling(rdsMetrics)...)
	case "cur.aws":
		alerts = append(alerts, curCost()...)
	case "cdr.asp":
		alerts = append(alerts, cdrFraud()...)
		alerts = append(alerts, cdrMargin()...)
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

func ec2awsFeedback(m *model, event *modEvt) {
	defer func() {
		if r := recover(); r != nil {
			logE.Printf("error looping in %q feedback for %q: %v", event.name, m.name, r)
		}
	}()

	switch event.name {
	case "cur.aws":
		type feedback struct {
			plat        string
			spot        bool
			usage, cost float32
		}
		ec2, det, active := m.newAcc(), m.data[1].(*ec2Detail), make(map[string]*feedback, 4096)
		func() {
			ec2.reqR()
			defer ec2.rel()
			for id, inst := range det.Inst {
				switch inst.State {
				case "running", "pending", "stopped":
					active[id] = nil
				}
			}
		}()
		func() {
			cur, now, pmap := mMod[event.name].newAcc(), "", map[string]string{
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
			defer cur.rel()
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
		}()
		func() {
			ec2.reqW()
			defer ec2.rel()
			for id, f := range active {
				if f == nil || f.usage == 0 {
				} else if inst := det.Inst[id]; inst != nil {
					inst.Plat = f.plat
					inst.ORate = f.cost / f.usage
					if f.spot && inst.Spot == "" {
						inst.Spot = "unknown SIR" // TODO: not known to occur in practice
					}
				}
			}
		}()
	}
}

func snapawsFeedback(m *model, event *modEvt) {
	defer func() {
		if r := recover(); r != nil {
			logE.Printf("error looping in %q feedback for %q: %v", event.name, m.name, r)
		}
	}()

	switch event.name {
	case "cur.aws":
		snap, det, size := m.newAcc(), m.data[1].(*snapDetail), make(map[string]float32, 4096)
		func() {
			snap.reqR()
			defer snap.rel()
			for id, s := range det.Snap {
				size[id] = s.Size
			}
		}()
		func() {
			cur, now := mMod[event.name].newAcc(), ""
			cur.reqR()
			defer cur.rel()
			for mo := range cur.m.data[1].(*curDetail).Line {
				if mo > now {
					now = mo
				}
			}
			for _, item := range cur.m.data[1].(*curDetail).Line[now] {
				if item.UOp != "CreateSnapshot" || !strings.HasPrefix(item.RID, "snapshot/") {
				} else if ss, found := size[item.RID[9:]]; !found {
				} else if cs := item.Usg / float32(item.Recs+1) * 365 / 12; cs > ss {
					// TODO: investigate calculations that better account for initial/final days
					size[item.RID[9:]] = cs
				}
			}
		}()
		func() {
			snap.reqW()
			defer snap.rel()
			for id, ss := range size {
				if ss == 0.0 {
				} else if s := det.Snap[id]; s != nil {
					s.Size = ss
				}
			}
		}()
	}
}
