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
		Location string

		ccR map[string]rateMap
	}

	// E164 ...
	E164 struct {
		Location string
		NANPbias bool
		Num      string
		Geo      string
		CC       string
		CCx      string
		CN       string
		ISO3166  string
		AC       string
		AN       string
		Sub      string

		decoder ccDecoder
	}

	// E164digest ...
	E164digest uint64 // E.164 low 50 bits; high 14 bits: geo code + CC, prefix & subscriber digits
)

// Load method on Rate...
func (r *Rate) Load(rr io.Reader) (err error) {
	res, b := make(ccRate), []byte{}
	if rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Location))
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
func (r *Rate) Lookup(tn *E164) (v float32) {
	if r == nil || tn == nil || tn.CC == "" || len(tn.Num) <= len(tn.CC) {
		return 0
	}
	rm := r.ccR[tn.CC]
	if rm == nil {
		return 0
	}
	for match := tn.Num[len(tn.CC):]; ; match = match[:len(match)-1] {
		if v = rm[match]; v > 0 {
			return v
		} else if match == "" {
			return rm["default"]
		}
	}
}

// Load method on E164 ...
func (tn *E164) Load(r io.Reader) (err error) {
	res, b := make(ccDecoder), []byte{}
	if r != nil {
		b, err = ioutil.ReadAll(r)
	} else if tn.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(tn.Location))
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
	n, intl, found := strings.Map(func(r rune) rune {
		switch r {
		case '(', ')', '[', ']', '-', '.', ' ', '\t':
			return -1
		}
		return r
	}, n), false, false
	if n[0] == '+' {
		n, intl = n[1:], true
	} else if strings.HasPrefix(n, "011") {
		n, intl = n[3:], true
	} else if strings.HasPrefix(n, "00") {
		n, intl = n[2:], true
	}

	var i e164Info
	if tn == nil {
		return ""
	} else if len(n) < 7 || len(n) > 15 {
		return tn.set(n, "", nil)
	} else if tn.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		i = tn.decoder["1"]
		return tn.set("1"+n, "1", &i)
	} else if i, found = tn.decoder[n[:1]]; found {
		return tn.set(n, n[:1], &i)
	} else if i, found = tn.decoder[n[:2]]; found {
		return tn.set(n, n[:2], &i)
	} else if i, found = tn.decoder[n[:3]]; found {
		return tn.set(n, n[:3], &i)
	}
	return tn.set(n, "", nil)
}

// Decode method on E164 ...
func (tn *E164) Decode(n string) string {
	// TODO: implement type switch to decode E164digest
	n, cc, intl, found := strings.Map(func(r rune) rune {
		switch r {
		case '(', ')', '[', ']', '-', '.', ' ', '\t':
			return -1
		}
		return r
	}, n), "", false, false
	if n[0] == '+' {
		n, intl = n[1:], true
	} else if strings.HasPrefix(n, "011") {
		n, intl = n[3:], true
	} else if strings.HasPrefix(n, "00") {
		n, intl = n[2:], true
	}

	var i e164Info
	if tn == nil {
		return ""
	} else if len(n) < 7 || len(n) > 15 {
		return tn.set(n, "", nil)
	} else if tn.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		n, cc, i = "1"+n, "1", tn.decoder["1"]
		//return tn.set(n, cc, &i)
	} else if i, found = tn.decoder[n[:1]]; found {
		cc = n[:1]
	} else if i, found = tn.decoder[n[:2]]; found {
		cc = n[:2]
	} else if i, found = tn.decoder[n[:3]]; found {
		cc = n[:3]
	} else {
		return tn.set(n, "", nil)
	}

	// expanded validation/decoding rules (including more precise area/subscriber partitioning)
	switch cc {
	case "1":
		// NANPA exceptions
	case "7":
		// Russia/Kazakhstan exceptions
	default:
	}
	return tn.set(n, cc, &i)
}

// Digest method on E164 ...
func (tn *E164) Digest(n string) E164digest {
	if tn == nil || n != "" && tn.QDecode(n) == "" || tn.Num == "" {
		return 0
	}
	// encode tn to E164digest
	return 0
}

// Geo method on E164digest ...
func (tn E164digest) Geo() string {
	// return lookup on uint64(tn) >> 60
	return ""
}

// Num64 method on E164digest ...
func (tn E164digest) Num64() uint64 {
	return uint64(tn) & 0x3_ffff_ffff_ffff
}

// set method on E164 (internal) ...
func (tn *E164) set(n string, cc string, i *e164Info) string {
	if i != nil && i.Geo != "" {
		tn.Num, tn.CC = n, cc
		tn.Geo = i.Geo
		tn.CN = i.CN
		tn.ISO3166 = i.ISO3166
		if so := len(cc) + i.AL; len(n) > so {
			tn.AC, tn.Sub = n[len(cc):so], n[so:]
		} else {
			tn.AC, tn.Sub = "", ""
		}
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CN, tn.ISO3166, tn.AC, tn.Sub = "", "", "", "", "", "", ""
	}
	// tn.CCx, tn.AN = "", "" // never set
	return tn.Num
}
