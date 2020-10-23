package aws

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

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
		Rate  float32
		Core  float32
		ECU   string
		Clock string
		Proc  string
		Feat  string
		Mem   string
		Sto   string
		EBS   string
		Net   string
	}
	// Rater ...
	Rater struct {
		Location string // JSON rate resource location (filename, ...)
		Default  string // default JSON rates

		kRV map[RateKey]RateValue
	}

	// EBSRateKey ...
	EBSRateKey struct {
		Region string
		Typ    string
	}
	// EBSRateValue ...
	EBSRateValue struct {
		SZrate float32
		IOrate float32
	}
	// EBSRater ...
	EBSRater struct {
		Location string // JSON rate resource location (filename, ...)
		Default  string // default JSON rates

		kRV map[EBSRateKey]EBSRateValue
	}
)

// Load method on Rater ...
func (r *Rater) Load(rr io.Reader, filter string) (err error) {
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
		if filter != "" && strings.HasPrefix(info.Typ, "db.") == (filter != "rds" && filter != "RDS") {
			continue
		}
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
	}
	if k.Plat == "" {
		k.Plat = "Lin"
	}
	if k.Terms == "" {
		k.Terms = "OD"
	}
	if k.Region == "" {
		k.Region = "us-east-1"
	} else if k.Region[len(k.Region)-1] > '9' {
		k.Region = k.Region[:len(k.Region)-1]
	}
	if v = r.kRV[*k]; v.Rate != 0.0 {
		return
	}
	k.Region = "us-east-1"
	return r.kRV[*k]
}

// Load method on EBSRater ...
func (r *EBSRater) Load(rr io.Reader) (err error) {
	res, b := []ebsRateInfo{}, []byte{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.kRV = nil; rr != nil {
		b, err = ioutil.ReadAll(rr)
	} else if r.Location != "" {
		b, err = ioutil.ReadFile(iio.ResolveName(r.Location))
	} else if r.Default != "" {
		b = []byte(r.Default)
	} else {
		b = []byte(defaultEBSRates)
	}
	if err != nil {
		return fmt.Errorf("cannot access rates resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("rates resource format problem: %v", err)
	}

	r.kRV = make(map[EBSRateKey]EBSRateValue)
	for _, info := range res {
		r.kRV[EBSRateKey{
			Region: info.Region,
			Typ:    info.Typ,
		}] = EBSRateValue{
			SZrate: info.SZrate / 730,
			IOrate: info.IOrate / 730,
		}
	}
	return nil
}

// Lookup method on EBSRater ...
func (r *EBSRater) Lookup(k *EBSRateKey) (v EBSRateValue) {
	if r == nil || k == nil {
		return
	}
	if k.Typ == "" {
		k.Typ = "gp2"
	}
	if k.Region == "" {
		k.Region = "us-east-1"
	} else if k.Region[len(k.Region)-1] > '9' {
		k.Region = k.Region[:len(k.Region)-1]
	}
	if v = r.kRV[*k]; v.SZrate != 0.0 {
		return
	}
	k.Region = "us-east-1"
	return r.kRV[*k]
}
