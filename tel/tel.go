package tel

import (
	"encoding/json"
	"io"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// Rate ...
	Rate struct {
		Name string
		cc   map[string]prefixRate
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
func (r *Rate) Load(rr *io.Reader) (err error) {
	res := new(rateRes)
	if rr != nil {
		// read rates from rr
	} else if r.Name != "" {
		iio.ResolveName(r.Name)
		// read rates from Name file
	} else {
		json.Unmarshal([]byte(defaultRates), res)
	}

	// migrate res.CC map to r.cc map
	r.cc = make(map[string]prefixRate)
	return nil
}

// Lookup ...
func (r *Rate) Lookup(tn *E164) float32 {
	if r == nil || r.cc == nil || tn == nil || tn.CC == "" || tn.AC == "" || tn.Sub == "" {
		return 0
	}
	return r.cc[tn.CC].pre[tn.AC] // placeholder
}

// Decode ...
func (n *E164) Decode() string {
	return ""
}
