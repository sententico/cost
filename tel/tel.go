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
		Location string // JSON rate resource location (filename, ...)
		Default  string // default JSON rates

		ccR map[string]pRate
	}

	// Decoder ...
	Decoder struct {
		Location string // encodings resource location (filename, ...)
		NANPbias bool   // set for NANP decoding bias

		ccI map[string]*ccInfo
	}

	// E164full ...
	E164full struct {
		Num     string // proper E.164 number
		Geo     string // geographic zone (with NANP subtypes)
		CC      string // country/service code
		CCx     string // country/service code extension
		CCn     string // country/service code name
		ISO3166 string // ISO 3166-2 alpha country code
		P       string // national-scope prefix (including area codes)
		Pn      string // national-scope prefix name
		Sub     string // subscriber number
	}

	// E164digest ...
	E164digest uint64 // high 14 bits: Geo code + CC, P & Sub digits; low 50 bits: E.164 Num
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
	if r == nil || tn == nil || tn.CC == "" || len(tn.Num) <= len(tn.CC) {
		return 0
	}
	pr := r.ccR[tn.CC]
	for match := tn.Num[len(tn.CC):]; ; match = match[:len(match)-1] {
		if v = pr[match]; v > 0 {
			return v
		} else if match == "" {
			return pr["default"]
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

	d.ccI = res
	return nil
}

// Quick method on Decoder ...
func (d *Decoder) Quick(n string, tn *E164full) error {
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
		return fmt.Errorf("missing E.164")
	} else if len(n) < 7 || len(n) > 15 {
		tn.set(n, "", nil)
		return fmt.Errorf("invalid E.164 length: %v", len(n))
	} else if d.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		tn.set("1"+n, "1", d.ccI["1"])
	} else if i := d.ccI[n[:1]]; i != nil {
		tn.set(n, n[:1], i)
	} else if i = d.ccI[n[:2]]; i != nil {
		tn.set(n, n[:2], i)
	} else if i = d.ccI[n[:3]]; i != nil {
		tn.set(n, n[:3], i)
	} else {
		tn.set(n, "", nil)
		return fmt.Errorf("no valid CC prefix")
	}
	return nil
}

// Full method on Decoder ...
func (d *Decoder) Full(n string, tn *E164full) error {
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
		return fmt.Errorf("missing E.164")
	} else if len(n) < 7 || len(n) > 15 {
		tn.set(n, "", nil)
		return fmt.Errorf("invalid E.164 length: %v", len(n))
	}

	var (
		i  *ccInfo
		cc string
	)
	if d.NANPbias && !intl && len(n) == 10 && n[0] != '0' && n[0] != '1' && n[3] != '0' && n[3] != '1' {
		n, cc, i = "1"+n, "1", d.ccI["1"]
	} else if i = d.ccI[n[:1]]; i != nil {
		cc = n[:1]
	} else if i = d.ccI[n[:2]]; i != nil {
		cc = n[:2]
	} else if i = d.ccI[n[:3]]; i != nil {
		cc = n[:3]
	} else {
		tn.set(n, "", nil)
		return fmt.Errorf("no valid CC prefix")
	}

	// expanded validation/decoding rules (including more precise P/Sub partitioning)
	switch cc {
	case "1":
		// NANPA exceptions
	case "7":
		// Russia/Kazakhstan exceptions
	default:
	}
	tn.set(n, cc, i)
	return nil
}

// Digest method on Decoder ...
func (d *Decoder) Digest(n string) E164digest {
	// TODO: code direct decoding for more efficiency
	if d == nil || n == "" {
		return 0
	} else if tn := (&E164full{}); d.Quick(n, tn) != nil {
		return 0
	} else if d, _ := strconv.ParseUint(tn.Num, 10, 64); d == 0 {
		return 0
	} else {
		d |= uint64(geoEncode[tn.Geo])<<60 | uint64(len(tn.CC))<<58 | uint64(len(tn.P))<<54 | uint64(len(tn.Sub))<<50
		return E164digest(d)
	}
}

// Digest method on E164full ...
func (tn *E164full) Digest() E164digest {
	// TODO: implement
	return 0
}

// Full method on E164digest ...
func (dtn E164digest) Full(d *Decoder, tn *E164full) error {
	// TODO: implement
	return nil
}

// Geo method on E164digest ...
func (dtn E164digest) Geo() string {
	return geoName[geoCode(uint64(dtn)>>60)]
}

// Num64 method on E164digest ...
func (dtn E164digest) Num64() uint64 {
	return uint64(dtn) & 0x3_ffff_ffff_ffff
}

// set method on E164full (internal) ...
func (tn *E164full) set(n string, cc string, i *ccInfo) {
	if i != nil {
		tn.Num, tn.CC = n, cc
		tn.Geo = i.Geo
		tn.CCn = i.CCn
		tn.ISO3166 = i.ISO3166
		if so := len(cc) + i.Pl; len(n) > so {
			tn.P, tn.Sub = n[len(cc):so], n[so:]
		} else {
			tn.P, tn.Sub = "", ""
		}
	} else {
		tn.Num, tn.CC, tn.Geo, tn.CCn, tn.ISO3166, tn.P, tn.Sub = "", "", "", "", "", "", ""
	}
	// tn.CCx, tn.Pn = "", "" // not used
}
