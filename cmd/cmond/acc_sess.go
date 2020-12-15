package main

import (
	"fmt"
	"net/url"
	"strings"
)

func ec2awsLookup(m *model, v url.Values, res chan<- interface{}) {
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

func ebsawsLookup(m *model, v url.Values, res chan<- interface{}) {
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

func rdsawsLookup(m *model, v url.Values, res chan<- interface{}) {
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

func accSeries(metric string, history, recent int, threshold float64) (res chan map[string][]float64, err error) {
	var acc *modAcc
	var sum interface{}
	var cur int32
	var typ byte
	if history <= 0 || history > 24*90 || recent <= 0 || recent > history || threshold < 0 {
		return nil, fmt.Errorf("invalid argument(s)")
	} else if acc = mMod[strings.Join(strings.SplitN(metric, ".", 3)[:2], ".")].newAcc(); acc == nil {
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
	case "cdr.asp/orig/cust", "cdr.asp/orig/cust/m", "cdr.asp/orig/cust/c", "cdr.asp/orig/cust/n", "cdr.asp/orig/cust/d":
		sum, cur = acc.m.data[1].(*origSum).ByCust, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/geo", "cdr.asp/orig/geo/m", "cdr.asp/orig/geo/c", "cdr.asp/orig/geo/n", "cdr.asp/orig/geo/d":
		sum, cur = acc.m.data[1].(*origSum).ByGeo, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/sp", "cdr.asp/orig/sp/m", "cdr.asp/orig/sp/c", "cdr.asp/orig/sp/n", "cdr.asp/orig/sp/d":
		sum, cur = acc.m.data[1].(*origSum).BySP, acc.m.data[1].(*origSum).Current
	case "cdr.asp/orig/loc", "cdr.asp/orig/loc/m", "cdr.asp/orig/loc/c", "cdr.asp/orig/loc/n", "cdr.asp/orig/loc/d":
		sum, cur = acc.m.data[1].(*origSum).ByLoc, acc.m.data[1].(*origSum).Current
	default:
		return nil, fmt.Errorf("unknown metric")
	}
	res = make(chan map[string][]float64, 1)
	go func() {
		defer func() {
			if e := recover(); e != nil && e.(error).Error() != "send on closed channel" {
				close(res)
			}
			acc.rel()
		}()
		rct, ser := make(map[string]float64), make(map[string][]float64)
		acc.reqR() // TODO: relocate prior to acc.m.data reference?
		for h := 0; h < recent; h++ {
			switch sum := sum.(type) {
			case hsU:
				if m := sum[cur-int32(h)]; m != nil {
					for n, i := range m {
						switch typ {
						case 'n':
							rct[n] += float64(i.Usage) / 3600
						default:
							rct[n] += i.Cost
						}
					}
				}
			case hsA:
				if m := sum[cur-int32(h)]; m != nil {
					for n, i := range m {
						rct[n] += i
					}
				}
			case hsC:
				if m := sum[cur-int32(h)]; m != nil {
					for n, i := range m {
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
			}
		}
		for n, t := range rct {
			if t >= threshold || -threshold >= t {
				ser[n] = make([]float64, 0, history)
			}
		}
		if len(rct) > 0 {
			switch sum := sum.(type) {
			case hsU:
				for h := 0; h < history; h++ {
					if m := sum[cur-int32(h)]; m != nil {
						for n, i := range m {
							if s := ser[n]; s != nil {
								switch s = s[:h+1]; typ {
								case 'n':
									s[h] = float64(i.Usage) / 3600
								default:
									s[h] = i.Cost
								}
							}
						}
					} else if h > recent {
						break
					}
				}
			case hsA:
				for h := 0; h < history; h++ {
					if m := sum[cur-int32(h)]; m != nil {
						for n, i := range m {
							if s := ser[n]; s != nil {
								s = s[:h+1]
								s[h] = i
							}
						}
					} else if h > recent {
						break
					}
				}
			case hsC:
				for h := 0; h < history; h++ {
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
							}
						}
					} else if h > recent {
						break
					}
				}
			}
		}
		acc.rel()
		res <- ser
		close(res)
	}()
	return
}
