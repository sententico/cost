package tel

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// Rate ...
	Rate struct {
		Res string
		ccR map[string]rateMap
	}

	// E164 ...
	E164 struct {
		Res      string
		NANPbias bool
		decoder  ccDecoder
		Num      string
		Geo      string
		CC       string
		CCx      string
		CN       string
		ISO3166  string
		AC       string
		AN       string
		Sub      string
	}
)

// Load method on Rate...
func (r *Rate) Load(rr io.Reader) (err error) {
	res, b := make(ccRate), []byte{}
	if rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Res != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Res))
	} else {
		b = []byte(defaultRates)
	}
	if err != nil {
		return
	} else if err = json.Unmarshal(b, &res); err != nil {
		return
	}

	r.ccR = make(map[string]rateMap)
	for cc, rgs := range res {
		rm := r.ccR[cc]
		if rm == nil {
			rm = make(rateMap)
			r.ccR[cc] = rm
		}
		for _, rg := range rgs {
			for _, pre := range rg.Pre {
				rm[pre] = rg.Rate
			}
		}
	}
	return nil
}

// Lookup method on Rate...
func (r *Rate) Lookup(tn *E164) float32 {
	if r == nil || r.ccR == nil || tn == nil || tn.CC == "" || len(tn.Num) <= len(tn.CC) {
		return 0
	}
	rm := r.ccR[tn.CC]
	if rm == nil {
		return 0
	}
	for match := tn.Num[len(tn.CC):]; len(match) > 0; match = match[:len(match)-1] {
		if r := rm[match]; r > 0 {
			return r
		}
	}
	return 0
}

// Load method on E164 ...
func (tn *E164) Load(r io.Reader) (err error) {
	res, b := make(ccDecoder), []byte{}
	if r != nil {
		b, err = ioutil.ReadAll(r)
	} else if tn.Res != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(tn.Res))
	} else {
		b = []byte(defaultDecoder)
	}
	if err != nil {
		return
	} else if err = json.Unmarshal(b, &res); err != nil {
		return
	}

	tn.decoder = res
	return nil
}

// QDecode method on E164 ...
func (tn *E164) QDecode(n string) string {
	n, intl := strings.Map(func(r rune) rune {
		switch r {
		case '(', ')', '[', ']', '-', '.', ' ', '\t':
			return -1
		}
		return r
	}, n), false
	if n[0] == '+' {
		n, intl = n[1:], true
	} else if strings.HasPrefix(n, "011") {
		n, intl = n[3:], true
	} else if strings.HasPrefix(n, "00") {
		n, intl = n[2:], true
	}

	if tn == nil {
		return ""
	} else if tn.decoder == nil || len(n) < 7 || len(n) > 15 {
		return tn.set(n, "", nil)
	} else if tn.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		d := tn.decoder["1"]
		return tn.set("1"+n, "1", &d)
	}

	d, found := tn.decoder[n[:1]]
	if found {
		return tn.set(n, n[:1], &d)
	} else if d, found = tn.decoder[n[:2]]; found {
		return tn.set(n, n[:2], &d)
	} else if d, found = tn.decoder[n[:3]]; found {
		return tn.set(n, n[:3], &d)
	}
	return tn.set(n, "", nil)
}

// Decode method on E164 ...
func (tn *E164) Decode(n string) string {
	n, intl, cc := strings.Map(func(r rune) rune {
		switch r {
		case '(', ')', '[', ']', '-', '.', ' ', '\t':
			return -1
		}
		return r
	}, n), false, ""
	if n[0] == '+' {
		n, intl = n[1:], true
	} else if strings.HasPrefix(n, "011") {
		n, intl = n[3:], true
	} else if strings.HasPrefix(n, "00") {
		n, intl = n[2:], true
	}

	if tn == nil {
		return ""
	} else if tn.decoder == nil || len(n) < 7 || len(n) > 15 {
		return tn.set(n, "", nil)
	} else if tn.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		d := tn.decoder["1"]
		return tn.set("1"+n, "1", &d)
	}

	d, found := tn.decoder[n[:1]]
	if found {
		cc = n[:1]
	} else if d, found = tn.decoder[n[:2]]; found {
		cc = n[:2]
	} else if d, found = tn.decoder[n[:3]]; found {
		cc = n[:3]
	} else {
		return tn.set(n, "", nil)
	}

	// expanded validation/decoding rules (including more precise area/subscriber partitioning)
	xd := d
	switch cc {
	case "1":
		// NANPA exceptions
	case "7":
		// Russia/Kazakhstan exceptions
	default:
	}
	return tn.set(n, cc, &xd)
}

// set method on E164 (internal) ...
func (tn *E164) set(n string, cc string, d *decodeItem) string {
	if d != nil && d.Geo != "" {
		tn.Num, tn.CC = n, cc
		tn.Geo = d.Geo
		tn.CN = d.CN
		tn.ISO3166 = d.ISO3166
		if so := len(cc) + d.AL; len(n) > so {
			tn.AC, tn.Sub = n[len(cc):so], n[so:]
		} else {
			tn.AC, tn.Sub = "", ""
		}
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CN, tn.ISO3166, tn.AC, tn.Sub = "", "", "", "", "", "", ""
	}
	tn.CCx, tn.AN = "", ""
	return tn.Num
}
