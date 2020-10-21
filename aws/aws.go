package aws

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	iio "github.com/sententico/cost/internal/io"
)

type (
	// RateKey ...
	RateKey struct {
		Region string
		Typ    string
		Plat   string
		Terms  string
	}

	// RateValue ...
	RateValue struct {
		Core  float32
		ECU   string
		Clock string
		Proc  string
		Feat  string
		Mem   string
		Sto   string
		EBS   string
		Net   string
		Rate  float32
	}

	// Rater ...
	Rater struct {
		Location string // JSON rate resource location (filename, ...)
		Default  string // default JSON rates

		kRV map[RateKey]RateValue
	}
)

// Load method on Rater ...
func (r *Rater) Load(rr io.Reader) (err error) {
	res, b := []rateInfo{}, []byte{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.kRV = nil; rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Location))
	} else if r.Default != "" {
		b = []byte(r.Default)
	} else {
		b = []byte(defaultRates)
	}
	if err != nil {
		return fmt.Errorf("cannot access rates resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("rates resource format problem: %v", err)
	}

	r.kRV = make(map[RateKey]RateValue)
	for _, info := range res {
		r.kRV[RateKey{
			Region: info.Region,
			Typ:    info.Typ,
			Plat:   info.Plat,
			Terms:  info.Terms,
		}] = RateValue{
			Core:  info.Core,
			ECU:   info.ECU,
			Clock: info.Clock,
			Proc:  info.Proc,
			Feat:  info.Feat,
			Mem:   info.Mem,
			Sto:   info.Sto,
			EBS:   info.EBS,
			Net:   info.Net,
			Rate:  info.Rate,
		}
	}
	return nil
}

// Lookup method on Rater ...
func (r *Rater) Lookup(k *RateKey) (v RateValue) {
	if r == nil || k == nil || k.Typ == "" {
		return
	} else if k.Region == "" || k.Plat == "" || k.Terms == "" {
		dk := *k
		if k.Region == "" {
			dk.Region = "us-east-1"
		}
		if k.Plat == "" {
			dk.Plat = "Lin"
		}
		if k.Terms == "" {
			dk.Terms = "OD"
		}
		k = &dk
	}
	return r.kRV[*k]
}
