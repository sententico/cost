package tel

import (
	"encoding/json"
	"io"
	"io/ioutil"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// Rate ...
	Rate struct {
		Name string
		ccm  map[string]prefixMap
	}

	// E164 ...
	E164 struct {
		Num      string
		NANPbias bool
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

// Load ...
func (r *Rate) Load(rr io.Reader) (err error) {
	res, b := make(rateRes), []byte{}
	if rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Name != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Name))
	} else {
		b = []byte(defaultRates)
	}
	if err != nil {
		return
	} else if err = json.Unmarshal(b, &res); err != nil {
		return
	}

	r.ccm = make(map[string]prefixMap)
	for cc, rgs := range res {
		pm := r.ccm[cc]
		if pm == nil {
			pm = make(prefixMap)
			r.ccm[cc] = pm
		}
		for _, rg := range rgs {
			for _, pre := range rg.Pre {
				pm[pre] = rg.Rate
			}
		}
	}
	return nil
}

// Lookup ...
func (r *Rate) Lookup(tn *E164) float32 {
	if r == nil || r.ccm == nil || tn == nil || tn.CC == "" || len(tn.Num) <= len(tn.CC) {
		return 0
	}
	pm := r.ccm[tn.CC]
	if pm == nil {
		return 0
	}
	for match := tn.Num[len(tn.CC):]; len(match) > 0; match = match[:len(match)-1] {
		if r := pm[match]; r > 0 {
			return r
		}
	}
	return 0
}

// Decode ...
func (tn *E164) Decode() string {
	return tn.Num // placeholder
}
