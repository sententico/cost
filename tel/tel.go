package tel

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// Rater ...
	Rater struct {
		Location    string  // JSON rate resource location (filename, ...)
		Default     string  // default JSON rates
		DefaultRate float32 // default rate

		ccR map[string]pRate
	}

	// Decoder ...
	Decoder struct {
		Location string // encodings resource location (filename, ...)
		NANPbias bool   // set for NANP decoding bias

		ccI map[string]*ccInfo
	}

	// SPmap service provider map ...
	SPmap struct {
		Location string // service provider resource location

		alCo map[string]uint16
		coNa map[uint16]string
	}

	// SLmap service location map ...
	SLmap struct {
		Location string // service location resource location

		alCo map[string]uint16
		coNa map[uint16]string
	}

	// E164full ...
	E164full struct {
		Num     string // proper E.164 number
		Geo     string // geographic zone (with NANP subtypes)
		CC      string // country/service code
		CCn     string // country/service code name
		ISO3166 string // ISO 3166-2 alpha country code (XC if n/a)
		P       string // national-scope prefix (including area codes)
		Pn      string // national-scope prefix name (unimplemented)
		Sub     string // subscriber suffix
	}

	// E164digest ...
	E164digest uint64 // high 50 bits: E.164 Num; low 14 bits: CC len | P len | Sub len | Geo code
)

// Load method on Rater ...
func (r *Rater) Load(rr io.Reader) (err error) {
	res, b := make(map[string][]rateGrp), []byte{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.ccR = nil; rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Location))
	} else if r.Default != "" {
		b = []byte(r.Default)
	} else {
		b = []byte(DefaultTermRates)
	}
	if err != nil {
		return fmt.Errorf("cannot access rates resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("rates resource format problem: %v", err)
	}

	r.ccR = make(map[string]pRate)
	for cc, rgs := range res {
		pr := r.ccR[cc]
		if pr == nil {
			pr = make(pRate)
			r.ccR[cc] = pr
		}
		for _, rg := range rgs {
			for _, p := range rg.P {
				pr[p] = rg.Rate
			}
		}
	}
	return nil
}

// Lookup method on Rater ...
func (r *Rater) Lookup(tn *E164full) (v float32) {
	if r == nil {
		return 0
	} else if tn == nil || tn.CC == "" || len(tn.Num) <= len(tn.CC) {
		return r.DefaultRate
	}
	pr := r.ccR[tn.CC]
	for match := tn.Num[len(tn.CC):]; ; match = match[:len(match)-1] {
		if v = pr[match]; v > 0 {
			return v
		} else if match == "" {
			if v = pr["default"]; v > 0 {
				return v
			} else {
				return r.DefaultRate
			}
		}
	}
}

// Load method on Decoder ...
func (d *Decoder) Load(dr io.Reader) (err error) {
	res, b := make(map[string]*ccInfo), []byte{}
	if d == nil {
		return fmt.Errorf("no decoder specified")
	} else if d.ccI = nil; dr != nil {
		b, err = ioutil.ReadAll(dr)
	} else if d.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(d.Location))
	} else {
		b = []byte(defaultEncodings)
	}
	if err != nil {
		return fmt.Errorf("cannot access encodings resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("encodings resource format problem: %v", err)
	}

	for _, i := range res {
		if len(i.Sub) > 0 {
			i.subI = make(map[string]*ccInfo)
			for _, si := range i.Sub {
				for _, p := range si.P {
					i.subI[p] = si
				}
			}
		}
	}
	d.ccI = res
	return nil
}

// Full method on Decoder ...
func (d *Decoder) Full(n string, tn *E164full) error {
	n, intl := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '(', ')', '-', '.', '[', ']', '\t':
			return -1
		}
		return r
	}, n), false
	if len(n) > 0 && n[0] == '+' {
		n, intl = n[1:], true
	} else if len(n) > 2 && n[:3] == "011" {
		n, intl = n[3:], true
	} else if len(n) > 1 && n[:2] == "00" {
		n, intl = n[2:], true
	}

	var cc string
	if tn == nil {
		return fmt.Errorf("missing E.164 target")
	} else if d == nil {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
		return fmt.Errorf("no E.164 decoder specified")
	} else if n := strings.Map(func(r rune) rune {
		switch r {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return r
		}
		return -1
	}, n); (len(n) < 8 || len(n) > 15) && (len(n) != 7 || n[:3] != "290") {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
		return fmt.Errorf("invalid E.164 filtered length: %v", len(n))
	} else if d.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[1] != '9' &&
		n[:2] != "37" && n[:2] != "96" && n[:3] != "555" && n[:3] != "950" && n[1:3] != "11" &&
		n[3] != '0' && n[3] != '1' {
		n, cc = "1"+n, "1"
	} else if d.ccI[n[:1]] != nil {
		cc = n[:1]
	} else if d.ccI[n[:2]] != nil {
		cc = n[:2]
	} else if d.ccI[n[:3]] != nil {
		cc = n[:3]
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
		return fmt.Errorf("prefix [%v]%v not a valid E.164 CC", n[:3], n[3:])
	}

	if i, p, s := d.ccInfo(n, cc); i == nil || s == "" {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
		return fmt.Errorf("cannot decode E.164 suffix %v[%v]", cc, n[len(cc):])
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = n, cc, i.Geo, i.CCn, i.ISO3166, p, s
		return nil
	}
}

// Digest method on Decoder ...
func (d *Decoder) Digest(n string) E164digest {
	n, intl := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '(', ')', '-', '.', '[', ']', '\t':
			return -1
		}
		return r
	}, n), false
	if len(n) > 0 && n[0] == '+' {
		n, intl = n[1:], true
	} else if len(n) > 2 && n[:3] == "011" {
		n, intl = n[3:], true
	} else if len(n) > 1 && n[:2] == "00" {
		n, intl = n[2:], true
	}

	var cc string
	np, _ := strconv.ParseUint(n, 10, 64)
	if d == nil || np == 0 || (len(n) < 8 || len(n) > 15) && (len(n) != 7 || n[:3] != "290") {
		return 0
	} else if d.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[1] != '9' &&
		n[:2] != "37" && n[:2] != "96" && n[:3] != "555" && n[:3] != "950" && n[1:3] != "11" &&
		n[3] != '0' && n[3] != '1' {
		n, cc = "1"+n, "1"
	} else if d.ccI[n[:1]] != nil {
		cc = n[:1]
	} else if d.ccI[n[:2]] != nil {
		cc = n[:2]
	} else if d.ccI[n[:3]] != nil {
		cc = n[:3]
	} else {
		return 0
	}

	if i, p, s := d.ccInfo(n, cc); i == nil || s == "" {
		return 0
	} else {
		return E164digest(np<<numShift | uint64(len(cc))<<ccShift | uint64(len(p))<<pShift | uint64(len(s))<<subShift | uint64(geoEncode[i.Geo])&geoMask)
	}
}

// Load method on SPmap ...
func (sp *SPmap) Load(r io.Reader) (err error) {
	res, b := make(map[uint16]spIDs), []byte{}
	if sp == nil {
		return fmt.Errorf("no service provider map specified")
	} else if sp.alCo, sp.coNa = nil, nil; r != nil {
		b, err = ioutil.ReadAll(r)
	} else if sp.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(sp.Location))
	} else {
		b = []byte(defaultProviders)
	}
	if err != nil {
		return fmt.Errorf("cannot access service provider resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("service provider resource format problem: %v", err)
	}

	sp.alCo, sp.coNa = make(map[string]uint16), make(map[uint16]string)
	for c, id := range res {
		sp.coNa[c], sp.alCo[id.Name] = id.Name, c
		for _, al := range id.Alias {
			sp.alCo[al] = c
		}
	}
	return nil
}

// Code method on SPmap ...
func (sp *SPmap) Code(al string) uint16 {
	return sp.alCo[al]
}

// Name method on SPmap ...
func (sp *SPmap) Name(co uint16) string {
	return sp.coNa[co]
}

// Load method on SLmap ...
func (sl *SLmap) Load(r io.Reader) (err error) {
	res, b := make(map[uint16]spIDs), []byte{}
	if sl == nil {
		return fmt.Errorf("no service location map specified")
	} else if sl.alCo, sl.coNa = nil, nil; r != nil {
		b, err = ioutil.ReadAll(r)
	} else if sl.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(sl.Location))
	} else {
		b = []byte(defaultLocations)
	}
	if err != nil {
		return fmt.Errorf("cannot access service location resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("service location resource format problem: %v", err)
	}

	sl.alCo, sl.coNa = make(map[string]uint16), make(map[uint16]string)
	for c, id := range res {
		sl.coNa[c], sl.alCo[id.Name] = id.Name, c
		for _, al := range id.Alias {
			sl.alCo[al] = c
		}
	}
	return nil
}

// Code method on SLmap ...
func (sl *SLmap) Code(al string) uint16 {
	return sl.alCo[al]
}

// Name method on SLmap ...
func (sl *SLmap) Name(co uint16) string {
	return sl.coNa[co]
}

// Digest method on E164full ...
func (tn *E164full) Digest(pre int) E164digest {
	if tn == nil || pre < len(tn.CC) || pre > len(tn.Num) || len(tn.CC) == 0 || len(tn.CC) > 3 ||
		len(tn.Num) < len(tn.CC) || len(tn.Num) > 15 || len(tn.CC)+len(tn.P)+len(tn.Sub) != len(tn.Num) {
		return 0
	} else if np, _ := strconv.ParseUint(tn.Num[:pre], 10, 64); np == 0 {
		return 0
	} else if pre < len(tn.Num) {
		return E164digest(np<<numShift | uint64(len(tn.CC))<<ccShift | uint64(pre-len(tn.CC))<<pShift | uint64(geoEncode[tn.Geo])&geoMask)
	} else {
		return E164digest(np<<numShift | uint64(len(tn.CC))<<ccShift | uint64(len(tn.P))<<pShift | uint64(len(tn.Sub))<<subShift | uint64(geoEncode[tn.Geo])&geoMask)
	}
}

// Full method on E164digest ...
func (tnd E164digest) Full(d *Decoder, tn *E164full) error {
	if tn == nil {
		return fmt.Errorf("missing E.164 target")
	} else if n, ccl, pl, subl := strconv.FormatUint(uint64(tnd>>numShift), 10), int(tnd>>ccShift&ccMask), int(tnd>>pShift&pMask),
		int(tnd>>subShift&subMask); ccl == 0 || len(n) > 15 || ccl+pl+subl != len(n) {
		return fmt.Errorf("invalid E.164 digest")
	} else if d == nil {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = n, n[:ccl], tnd.Geo(), "", "", n[ccl:ccl+pl], n[ccl+pl:]
	} else if i, _, _ := d.ccInfo(n, n[:ccl]); i == nil {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
		return fmt.Errorf("cannot decode %q as E.164 CC", n[:ccl])
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = n, n[:ccl], i.Geo, i.CCn, i.ISO3166, n[ccl:ccl+pl], n[ccl+pl:]
	}
	return nil
}

// Geo method on E164digest ...
func (tnd E164digest) Geo() string {
	return geoDecode[geoCode(tnd&geoMask)]
}

// Num64 method on E164digest ...
func (tnd E164digest) Num64() uint64 {
	return uint64(tnd) >> numShift
}
