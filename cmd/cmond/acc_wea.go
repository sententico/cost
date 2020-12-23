package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sententico/cost/tel"
)

const (
	pgSize = 256
)

var (
	weaselMap = map[string]string{
		"aws":   "wea_aws.py",
		"dd":    "wea_dd.py",
		"slack": "wea_slack.py",
		"":      "wea_test.py", // default weasel
	}
)

func releaseWeasel(service string, options ...string) *exec.Cmd {
	for suffix := service; ; suffix = suffix[1:] {
		if wea := weaselMap[suffix]; wea != "" {
			args := []string{
				"python",
				fmt.Sprintf("%v/%v", strings.TrimRight(settings.BinDir, "/"), wea),
			}
			// TODO: change to exec.CommandContext() to support timeouts?
			return exec.Command(args[0], append(append(args[1:], options...), service)...)
		} else if suffix == "" {
			return nil
		}
	}
}

func weasel(service string) (weain io.WriteCloser, weaout io.ReadCloser, err error) {
	var weaerr io.ReadCloser
	if wea := releaseWeasel(service); wea == nil {
		err = fmt.Errorf("unknown weasel: %q", service)
	} else if weain, err = wea.StdinPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q weasel: %v", service, err)
	} else if weaout, err = wea.StdoutPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q weasel: %v", service, err)
	} else if weaerr, err = wea.StderrPipe(); err != nil {
		err = fmt.Errorf("problem connecting to %q weasel: %v", service, err)
	} else if _, err = io.WriteString(weain, settings.JSON); err != nil {
		err = fmt.Errorf("setup problem with %q weasel: %v", service, err)
	} else if err = wea.Start(); err != nil {
		err = fmt.Errorf("%q weasel refused release: %v", service, err)
	} else {
		go func() {
			var em []byte
			if eb, _ := ioutil.ReadAll(weaerr); len(eb) == 0 {
				time.Sleep(250 * time.Millisecond) // give stdout opportunity to clear
			} else {
				el := bytes.Split(bytes.Trim(eb, "\n\t "), []byte("\n"))
				em = bytes.TrimLeft(el[len(el)-1], "\t ")
			}
			if e := wea.Wait(); e != nil {
				logE.Printf("%q weasel errors: %v [%s]", service, e, em)
			} else if len(em) > 0 {
				logE.Printf("%q weasel warnings: [%s]", service, em)
			}
		}()
		return
	}
	return nil, nil, err
}

func ec2awsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if inst := m.data[1].(*ec2Detail).Inst[id]; inst != nil {
		s = fmt.Sprintf("%v", *inst)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func ebsawsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if vol := m.data[1].(*ebsDetail).Vol[id]; vol != nil {
		s = fmt.Sprintf("%v", *vol)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func rdsawsLookupX(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if db := m.data[1].(*rdsDetail).DB[id]; db != nil {
		s = fmt.Sprintf("%v", *db)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func (sum hsU) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'n':
				rct[n] += float64(i.Usage) / 3600
			default:
				rct[n] += i.Cost
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'n':
							s[h] = float64(i.Usage) / 3600
						default:
							s[h] = i.Cost
						}
						ser[n] = s
					}
				}
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hsA) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			rct[n] += i
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						s = s[:h+1]
						s[h], ser[n] = i, s
					}
				}
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hsC) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, ser := make(map[string]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'm':
				rct[n] += i.Marg
			case 'c':
				rct[n] += i.Bill - i.Marg
			case 'n':
				rct[n] += float64(i.Calls)
			case 'd':
				rct[n] += float64(i.Dur) / 600
			default:
				rct[n] += i.Bill
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			ser[n] = make([]float64, 0, span)
		}
	}
	if len(ser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := ser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'm':
							s[h] = i.Marg
						case 'c':
							s[h] = i.Bill - i.Marg
						case 'n':
							s[h] = float64(i.Calls)
						case 'd':
							s[h] = float64(i.Dur) / 600
						default:
							s[h] = i.Bill
						}
						ser[n] = s
					}
				}
			} else if h >= recent {
				break
			}
		}
	}
	return
}

func (sum hnC) extract(typ byte, cur int32, span, recent int, truncate float64) (ser map[string][]float64) {
	rct, nser, ser := make(map[tel.E164digest]float64), make(map[tel.E164digest][]float64), make(map[string][]float64)
	for h := 0; h < recent; h++ {
		for n, i := range sum[cur-int32(h)] {
			switch typ {
			case 'm':
				rct[n] += i.Marg
			case 'c':
				rct[n] += i.Bill - i.Marg
			case 'n':
				rct[n] += float64(i.Calls)
			case 'd':
				rct[n] += float64(i.Dur) / 600
			default:
				rct[n] += i.Bill
			}
		}
	}
	for n, t := range rct {
		if t >= truncate || -truncate >= t {
			nser[n] = make([]float64, 0, span)
		}
	}
	if len(nser) > 0 {
		for h := 0; h < span; h++ {
			if m := sum[cur-int32(h)]; m != nil {
				for n, i := range m {
					if s := nser[n]; s != nil {
						switch s = s[:h+1]; typ {
						case 'm':
							s[h] = i.Marg
						case 'c':
							s[h] = i.Bill - i.Marg
						case 'n':
							s[h] = float64(i.Calls)
						case 'd':
							s[h] = float64(i.Dur) / 600
						default:
							s[h] = i.Bill
						}
						nser[n] = s
					}
				}
			} else if h >= recent {
				break
			}
		}
		var e164 tel.E164full
		for n, s := range nser {
			n.Full(nil, &e164)
			ser[fmt.Sprintf("+%v %v[%v]", e164.CC, e164.P, e164.Geo)] = s
		}
	}
	return
}

func seriesExtract(metric string, span, recent int, truncate float64) (res chan map[string][]float64, err error) {
	var acc *modAcc
	var sum interface{}
	var cur int32
	var typ byte
	if span <= 0 || span > 24*90 || recent <= 0 || recent > span || truncate < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(strings.SplitN(metric, "/", 2)[0], ".", 3)[:2], ".")].newAcc(); acc == nil {
		return nil, fmt.Errorf("model not found")
	} else if metric[len(metric)-2] == '/' {
		typ = metric[len(metric)-1]
	}

	switch metric {
	case "ec2.aws/acct", "ec2.aws/acct/s":
		sum, cur = acc.m.data[0].(*ec2Sum).ByAcct, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/region", "ec2.aws/region/s":
		sum, cur = acc.m.data[0].(*ec2Sum).ByRegion, acc.m.data[0].(*ec2Sum).Current
	case "ec2.aws/sku", "ec2.aws/sku/s":
		sum, cur = acc.m.data[0].(*ec2Sum).BySKU, acc.m.data[0].(*ec2Sum).Current

	case "ebs.aws/acct", "ebs.aws/acct/s":
		sum, cur = acc.m.data[0].(*ebsSum).ByAcct, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/region", "ebs.aws/region/s":
		sum, cur = acc.m.data[0].(*ebsSum).ByRegion, acc.m.data[0].(*ebsSum).Current
	case "ebs.aws/sku", "ebs.aws/sku/s":
		sum, cur = acc.m.data[0].(*ebsSum).BySKU, acc.m.data[0].(*ebsSum).Current

	case "rds.aws/acct", "rds.aws/acct/s":
		sum, cur = acc.m.data[0].(*rdsSum).ByAcct, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/region", "rds.aws/region/s":
		sum, cur = acc.m.data[0].(*rdsSum).ByRegion, acc.m.data[0].(*rdsSum).Current
	case "rds.aws/sku", "rds.aws/sku/s":
		sum, cur = acc.m.data[0].(*rdsSum).BySKU, acc.m.data[0].(*rdsSum).Current

	case "cur.aws/acct":
		sum, cur = acc.m.data[0].(*curSum).ByAcct, acc.m.data[0].(*curSum).Current
	case "cur.aws/region":
		sum, cur = acc.m.data[0].(*curSum).ByRegion, acc.m.data[0].(*curSum).Current
	case "cur.aws/typ":
		sum, cur = acc.m.data[0].(*curSum).ByTyp, acc.m.data[0].(*curSum).Current
	case "cur.aws/svc":
		sum, cur = acc.m.data[0].(*curSum).BySvc, acc.m.data[0].(*curSum).Current

	case "cdr.asp/term/cust", "cdr.asp/term/cust/m", "cdr.asp/term/cust/c", "cdr.asp/term/cust/n", "cdr.asp/term/cust/d":
		sum, cur = acc.m.data[0].(*termSum).ByCust, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/geo", "cdr.asp/term/geo/m", "cdr.asp/term/geo/c", "cdr.asp/term/geo/n", "cdr.asp/term/geo/d":
		sum, cur = acc.m.data[0].(*termSum).ByGeo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/sp", "cdr.asp/term/sp/m", "cdr.asp/term/sp/c", "cdr.asp/term/sp/n", "cdr.asp/term/sp/d":
		sum, cur = acc.m.data[0].(*termSum).BySP, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/loc", "cdr.asp/term/loc/m", "cdr.asp/term/loc/c", "cdr.asp/term/loc/n", "cdr.asp/term/loc/d":
		sum, cur = acc.m.data[0].(*termSum).ByLoc, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/to", "cdr.asp/term/to/m", "cdr.asp/term/to/c", "cdr.asp/term/to/n", "cdr.asp/term/to/d":
		sum, cur = acc.m.data[0].(*termSum).ByTo, acc.m.data[0].(*termSum).Current
	case "cdr.asp/term/from", "cdr.asp/term/from/m", "cdr.asp/term/from/c", "cdr.asp/term/from/n", "cdr.asp/term/from/d":
		sum, cur = acc.m.data[0].(*termSum).ByFrom, acc.m.data[0].(*termSum).Current

	case "cdr.asp/orig/cust", "cdr.asp/orig/cust/m", "cdr.asp/orig/cust/c", "cdr.asp/orig/cust/n", "cdr.asp/orig/cust/d":
		sum, cur = acc.m.data[1].(*origSum).ByCust, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/geo", "cdr.asp/orig/geo/m", "cdr.asp/orig/geo/c", "cdr.asp/orig/geo/n", "cdr.asp/orig/geo/d":
		sum, cur = acc.m.data[1].(*origSum).ByGeo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/sp", "cdr.asp/orig/sp/m", "cdr.asp/orig/sp/c", "cdr.asp/orig/sp/n", "cdr.asp/orig/sp/d":
		sum, cur = acc.m.data[1].(*origSum).BySP, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/loc", "cdr.asp/orig/loc/m", "cdr.asp/orig/loc/c", "cdr.asp/orig/loc/n", "cdr.asp/orig/loc/d":
		sum, cur = acc.m.data[1].(*origSum).ByLoc, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/to", "cdr.asp/orig/to/m", "cdr.asp/orig/to/c", "cdr.asp/orig/to/n", "cdr.asp/orig/to/d":
		sum, cur = acc.m.data[1].(*origSum).ByTo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/from", "cdr.asp/orig/from/m", "cdr.asp/orig/from/c", "cdr.asp/orig/from/n", "cdr.asp/orig/from/d":
		sum, cur = acc.m.data[1].(*origSum).ByFrom, acc.m.data[1].(*origSum).Current

	default:
		return nil, fmt.Errorf("unknown metric")
	}
	res = make(chan map[string][]float64, 1)

	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer func() { recover() }()
				close(res)
			}
		}()
		var ser map[string][]float64
		acc.reqR() // TODO: may relocate prior to acc.m.data reference
		switch sum := sum.(type) {
		case hsU:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hsA:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hsC:
			ser = sum.extract(typ, cur, span, recent, truncate)
		case hnC:
			ser = sum.extract(typ, cur, span, recent, truncate)
		}
		acc.rel()
		res <- ser
		close(res)
	}()
	return
}

func curawsExtract(from, to int32, units int16, items int, truncate float64) (res chan []string, err error) {
	var acc *modAcc
	if items++; from > to || units < 1 || items < 0 || items == 1 || truncate < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod["cur.aws"].newAcc(); acc == nil {
		return nil, fmt.Errorf("\"cur.aws\" model not found")
	}

	res = make(chan []string, 32)
	go func() {
		defer func() {
			acc.rel()
			if e := recover(); e != nil && !strings.HasSuffix(e.(error).Error(), "closed channel") {
				logE.Printf("error while accessing %q: %v", acc.m.name, e)
				defer func() { recover() }()
				close(res)
			}
		}()
		pg, threshold := pgSize, float32(truncate)
		acc.reqR()

	nextMonth:
		for mo, hrs := range acc.m.data[1].(*curDetail).Month {
			if to >= hrs[0] && hrs[1] >= from {
				dts := mo[:4] + "-" + mo[4:] + "-01" // +" "+hh+":00"
				for id, li := range acc.m.data[1].(*curDetail).Line[mo] {
					if li.Cost < threshold && -threshold < li.Cost {
						continue
					} else if items--; items == 0 {
						break nextMonth
					}

					ls := []string{
						dts[:8] + id,
						dts,
						li.Acct,
						li.Typ,
						li.Svc,
						li.UTyp,
						li.UOp,
						li.Reg,
						li.RID,
						li.Desc,
						li.Name,
						li.Env,
						li.DC,
						li.Prod,
						li.App,
						li.Cust,
						li.Team,
						li.Ver,
						strconv.FormatInt(int64(li.Mu+1), 10),
						strconv.FormatFloat(float64(li.Usg), 'g', -1, 32),
						strconv.FormatFloat(float64(li.Cost), 'g', -1, 32),
					}
					if pg--; pg >= 0 {
						select {
						case res <- ls:
							continue
						default:
						}
					}
					acc.rel()
					res <- ls
					pg = pgSize
					acc.reqR()
				}
			}
		}
		acc.rel()
		close(res)
	}()
	return
}
