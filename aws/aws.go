package aws

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	iio "github.com/sententico/cost/internal/io"
)

const (
	// DefaultRDSEBSRates ... requires maintenance updates (last Feb22)
	DefaultRDSEBSRates = `[
		{"Rg":"us-east-1",	"T":"gp2",		"SZ":0.115,	"IO":0},
		{"Rg":"us-east-1",	"T":"io1",		"SZ":0.125,	"IO":0.1},
		{"Rg":"us-east-1",	"T":"io2",		"SZ":0.125,	"IO":0.1},
		{"Rg":"us-east-1",	"T":"st1",		"SZ":0.1,	"IO":0},
		{"Rg":"us-east-1",	"T":"standard",	"SZ":0.1,	"IO":0},
		{"Rg":"us-east-1",	"T":"aurora",	"SZ":0.1,	"IO":0},

		{"Rg":"us-west-1",	"T":"gp2",		"SZ":0.138,	"IO":0},
		{"Rg":"us-west-1",	"T":"io1",		"SZ":0.138,	"IO":0.11},
		{"Rg":"us-west-1",	"T":"io2",		"SZ":0.138,	"IO":0.11},
		{"Rg":"us-west-1",	"T":"st1",		"SZ":0.11,	"IO":0},
		{"Rg":"us-west-1",	"T":"standard",	"SZ":0.11,	"IO":0},
		{"Rg":"us-west-1",	"T":"aurora",	"SZ":0.11,	"IO":0},

		{"Rg":"eu-west-1",	"T":"gp2",		"SZ":0.127,	"IO":0},
		{"Rg":"eu-west-1",	"T":"io1",		"SZ":0.138,	"IO":0.11},
		{"Rg":"eu-west-1",	"T":"io2",		"SZ":0.138,	"IO":0.11},
		{"Rg":"eu-west-1",	"T":"st1",		"SZ":0.11,	"IO":0},
		{"Rg":"eu-west-1",	"T":"standard",	"SZ":0.11,	"IO":0},
		{"Rg":"eu-west-1",	"T":"aurora",	"SZ":0.11,	"IO":0},

		{"Rg":"eu-west-2",	"T":"gp2",		"SZ":0.133,	"IO":0},
		{"Rg":"eu-west-2",	"T":"io1",		"SZ":0.145,	"IO":0.116},
		{"Rg":"eu-west-2",	"T":"io2",		"SZ":0.145,	"IO":0.116},
		{"Rg":"eu-west-2",	"T":"st1",		"SZ":0.116,	"IO":0},
		{"Rg":"eu-west-2",	"T":"standard",	"SZ":0.116,	"IO":0},
		{"Rg":"eu-west-2",	"T":"aurora",	"SZ":0.1,	"IO":0},

		{"Rg":"eu-central-1","T":"gp2",		"SZ":0.133,	"IO":0},
		{"Rg":"eu-central-1","T":"io1",		"SZ":0.145,	"IO":0.116},
		{"Rg":"eu-central-1","T":"io2",		"SZ":0.145,	"IO":0.116},
		{"Rg":"eu-central-1","T":"st1",		"SZ":0.116,	"IO":0},
		{"Rg":"eu-central-1","T":"standard","SZ":0.116,	"IO":0},
		{"Rg":"eu-central-1","T":"aurora",	"SZ":0.119,	"IO":0}
	]`
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

		kRV map[RateKey]*RateValue
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

		kRV map[EBSRateKey]*EBSRateValue
	}
	// SnapRater ...
	SnapRater struct {
		Location string // JSON rate resource location (filename, ...)
		Default  string // default JSON rates

		kRV map[EBSRateKey]float32
	}
)

// requires maintenance updates (last Feb22)
var (
	platMap = map[string]string{
		"":                  "Lin", // or macOS
		"windows":           "Win",
		"rhel":              "RHEL",
		"aurora":            "AURm",
		"aurora-mysql":      "AURm",
		"aurora-postgresql": "AURp",
		"mariadb":           "MAR",
		"docdb":             "DOC",
		"mysql":             "MSQL",
		"postgres":          "PSQL",
		"oracle-ee":         "MSQL",  // BYOL matches MySQL pricing
		"oracle-se":         "MSQL",  // BYOL matches MySQL pricing; deprecated
		"oracle-se1":        "ORLs1", // deprecated
		"oracle-se2":        "ORLs2",
		"sqlserver-ee":      "SQLe",
		"sqlserver-se":      "SQLs",
		"sqlserver-web":     "SQLw",
		"sqlserver-ex":      "SQLx",
	}
)

// Load method on Rater ...
func (r *Rater) Load(rr io.Reader, filter string) (err error) {
	var b []byte
	res := []rateInfo{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.kRV = nil; rr != nil {
		b, err = io.ReadAll(rr)
	} else if r.Location != "" {
		b, err = os.ReadFile(iio.ResolveName(r.Location))
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

	r.kRV = make(map[RateKey]*RateValue, 2<<17)
	for _, info := range res {
		if filter != "" && strings.HasPrefix(info.Typ, "db.") == (filter != "rds" && filter != "RDS") {
			continue
		}
		r.kRV[RateKey{
			Region: info.Region,
			Typ:    info.Typ,
			Plat:   info.Plat,
			Terms:  info.Terms,
		}] = &RateValue{
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

// Region returns an AWS region string from an availability zone string
func Region(az string) string {
	if len(az) > 1 && az[len(az)-1] > '9' {
		return az[:len(az)-1]
	}
	return az
}

// Lookup method on Rater ...
func (r *Rater) Lookup(k *RateKey) (v *RateValue) {
	if r == nil || k == nil || k.Typ == "" {
		return
	}
	if p := platMap[k.Plat]; p != "" {
		k.Plat = p
	}
	if k.Terms == "" {
		k.Terms = "OD"
	}
	if len(k.Region) < 3 {
		k.Region = "us-east-1"
	} else if k.Region[len(k.Region)-1] > '9' {
		k.Region = k.Region[:len(k.Region)-1]
	}
	if v = r.kRV[*k]; v != nil || k.Region == "us-east-1" || k.Region == "eu-west-1" {
		return
	}
	switch k.Region[:3] {
	case "us-":
		k.Region = "us-east-1"
	default:
		k.Region = "eu-west-1"
	}
	return r.kRV[*k]
}

// Load method on EBSRater ...
func (r *EBSRater) Load(rr io.Reader) (err error) {
	var b []byte
	res := []ebsRateInfo{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.kRV = nil; rr != nil {
		b, err = io.ReadAll(rr)
	} else if r.Location != "" {
		b, err = os.ReadFile(iio.ResolveName(r.Location))
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

	r.kRV = make(map[EBSRateKey]*EBSRateValue)
	for _, info := range res {
		r.kRV[EBSRateKey{
			Region: info.Region,
			Typ:    info.Typ,
		}] = &EBSRateValue{
			SZrate: info.SZrate / 730,
			IOrate: info.IOrate / 730,
		}
	}
	return nil
}

// Lookup method on EBSRater ...
func (r *EBSRater) Lookup(k *EBSRateKey) (v *EBSRateValue) {
	if r == nil || k == nil {
		return
	}
	if k.Typ == "" {
		k.Typ = "gp3"
	}
	if len(k.Region) < 3 {
		k.Region = "us-east-1"
	} else if k.Region[len(k.Region)-1] > '9' {
		k.Region = k.Region[:len(k.Region)-1]
	}
	if v = r.kRV[*k]; v != nil || k.Region == "us-east-1" || k.Region == "eu-west-1" {
		return
	}
	switch k.Region[:3] {
	case "us-":
		k.Region = "us-east-1"
	default:
		k.Region = "eu-west-1"
	}
	return r.kRV[*k]

}

// Load method on SnapRater ...
func (r *SnapRater) Load(rr io.Reader) (err error) {
	var b []byte
	res := []snapRateInfo{}
	if r == nil {
		return fmt.Errorf("no rater specified")
	} else if r.kRV = nil; rr != nil {
		b, err = io.ReadAll(rr)
	} else if r.Location != "" {
		b, err = os.ReadFile(iio.ResolveName(r.Location))
	} else if r.Default != "" {
		b = []byte(r.Default)
	} else {
		b = []byte(defaultSnapRates)
	}
	if err != nil {
		return fmt.Errorf("cannot access rates resource: %v", err)
	} else if err = json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("rates resource format problem: %v", err)
	}

	r.kRV = make(map[EBSRateKey]float32)
	for _, info := range res {
		r.kRV[EBSRateKey{
			Region: info.Region,
			Typ:    info.Typ,
		}] = info.SZrate / 730
	}
	return nil
}

// Lookup method on SnapRater ...
func (r *SnapRater) Lookup(k *EBSRateKey) (v float32) {
	if r == nil || k == nil {
		return
	}
	if k.Typ == "" {
		k.Typ = "standard"
	}
	if len(k.Region) < 3 {
		k.Region = "us-east-1"
	} else if k.Region[len(k.Region)-1] > '9' {
		k.Region = k.Region[:len(k.Region)-1]
	}
	if v = r.kRV[*k]; v != 0 || k.Region == "us-east-1" || k.Region == "eu-west-1" {
		return
	}
	switch k.Region[:3] {
	case "us-":
		k.Region = "us-east-1"
	default:
		k.Region = "eu-west-1"
	}
	return r.kRV[*k]

}
