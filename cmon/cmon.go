package cmon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Cloud Monitor settings types
type (
	// TagMap ...
	TagMap map[string]string

	// MetricMap ...
	MetricMap map[string][]float32

	// alertsFeature settings
	alertsFeature struct {
		Options             string
		Profiles, Customers map[string]map[string]string
	}

	// varianceFeature settings
	VarEnvironment struct {
		EC2     map[string][]int
		Filters map[string][]string
		Nre     *regexp.Regexp
	}
	VarTemplate struct {
		Descr string
		Envs  map[string]*VarEnvironment
		EC2   map[string][]int
	}
	VarVolume struct {
		SType string
		GiB   []float32
		IOPS  float32
	}
	VarInstance struct {
		Descr string
		Match string
		Plat  string
		IType string
		Vols  map[string]*VarVolume
		Mre   *regexp.Regexp
	}
	varianceFeature struct {
		Options   string
		Templates map[string]*VarTemplate
		EC2       map[string]*VarInstance
	}

	// awsService settings
	awsService struct {
		Options                            string
		SavPlan                            string
		SavCov, SpotDisc, UsageAdj, EDPAdj float32
		CUR                                map[string]string
		SES                                map[string]string
		TagRules                           map[string]map[string]map[string][]string
		Profiles                           map[string]map[string]float32
		Regions, Accounts                  map[string]TagMap
		tcmap                              map[string]map[string]map[string][2]string
		tpmap                              map[string]map[string][]*regexp.Regexp
	}
	// datadogService settings
	datadogService struct {
		Options        string
		APIKey, AppKey string
	}
	// slackService settings
	slackService struct {
		Options  string
		Webhooks map[string]string
	}

	// MonSettings are composite settings for Cloud Monitor
	MonSettings struct {
		Autoload        bool
		Options         string
		Unit, Address   string
		WorkDir, BinDir string
		Models          map[string]string
		Alerts          alertsFeature
		Variance        varianceFeature
		AWS             awsService
		Datadog         datadogService
		Slack           slackService
		JSON            string `json:"-"`
		loc             string
		ltime           time.Time
	}
)

// Cloud Monitor API argument/return types
type (
	// AuthArgs ...
	AuthArgs struct {
		ID   string // identification token
		Hash string // SHA256 of RFC3339 GMT (YYYY-MM-DDThh:mm) concatenated with secret token
	}

	// LookupArgs ...
	LookupArgs struct {
		Token string // Admin.Auth access token (renew hourly to avoid expiration)
		Key   string // lookup key
	}

	// SeriesArgs ...
	SeriesArgs struct {
		Token    string  // Admin.Auth access token (renew hourly to avoid expiration)
		Metric   string  // metric type
		Span     int     // total hours in series to return
		Recent   int     // recent/active hours in series
		Truncate float64 // filter metrics with recent amounts below this in absolute value
	}

	// SeriesRet ...
	SeriesRet struct {
		From   int32                // most recent hour in series (hours in Unix epoch)
		Series map[string][]float64 // map of metric values to hourly amounts
	}

	// TableArgs ...
	TableArgs struct {
		Token    string   // Admin.Auth access token (renew hourly to avoid expiration)
		Model    string   // model name
		Rows     int      // maximum rows
		Criteria []string // filter criteria (column/operator/operand tuples)
	}

	// CURtabArgs ...
	CURtabArgs struct {
		Token    string   // Admin.Auth access token (renew hourly to avoid expiration)
		From     int32    // from hour
		To       int32    // to hour
		Units    int16    // item reporting units (hour=1, day=24, month=720)
		Rows     int      // maximum line items
		Truncate float64  // filter items with costs below this in absolute value
		Criteria []string // filter criteria (column/operator/operand tuples)
	}

	// VarianceArgs ...
	VarianceArgs struct {
		Token    string // Admin.Auth access token (renew hourly to avoid expiration)
		Rows     int    // maximum rows
		Nofilter bool   // filter bypass
	}
)

var (
	mutex sync.Mutex
)

// Getarg is a helper function ...
func Getarg(v []string) string {
	for _, arg := range v {
		if strings.HasPrefix(arg, "CMON_") {
			arg = os.Getenv(arg)
		}
		if arg != "" {
			return arg
		}
	}
	return ""
}

// resolveLoc is a helper function that resolves resource location names (pathnames, ...)
func resolveLoc(n string) string {
	if strings.HasPrefix(n, "~/") {
		if u, err := user.Current(); err == nil {
			if p, err := filepath.Abs(u.HomeDir + n[1:]); err == nil {
				return p
			}
		}
	} else if strings.HasPrefix(n, "/") || strings.HasPrefix(n, "./") || strings.HasPrefix(n, "../") {
	} else if p, err := filepath.Abs(n); err == nil {
		d, f := filepath.Split(p)
		for _, err = os.Stat(p); os.IsNotExist(err); _, err = os.Stat(p) {
			if d, _ = filepath.Split(d[:len(d)-1]); d == "" {
				return n
			}
			p = d + f
		}
		return p
	}
	return n
}

// getMM is a helper function that gets min/max values in a slice, reducing to a min/max pair
func getMM[M int | float32](mms []M) []M {
	mm := []M{0, 0}
	if len(mms) > 0 {
		sort.Slice(mms, func(i, j int) bool { return mms[i] < mms[j] })
		if max := mms[len(mms)-1]; mms[0] > 0 {
			mm[0], mm[1] = mms[0], max
		} else if max > 0 {
			mm[1] = max
		}
	}
	return mm
}

// Reload Cloud Monitor settings from location or non-blocking function source
func Reload(cur **MonSettings, source interface{}) (loaded bool, err error) {
	var new *MonSettings
	var b []byte
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("problem digesting settings: %v", r)
		}
	}()

	switch s := source.(type) {
	case string: // (re)load settings from location (file, ...) source
		var bb bytes.Buffer
		if cur == nil || *cur == nil && s == "" {
			return false, fmt.Errorf("no settings specified")
		} else if err = func() (e error) {
			var fi fs.FileInfo
			if s == "" {
				if fi, _ = os.Stat((**cur).loc); fi == nil || fi.ModTime() == (**cur).ltime {
					return // no reloadable update available
				} else if (**cur).ltime = fi.ModTime(); false { // treat as if loaded
				} else if b, e = os.ReadFile((**cur).loc); e != nil {
					return fmt.Errorf("cannot read settings %q: %v", (**cur).loc, e)
				}
				new = &MonSettings{loc: (**cur).loc, ltime: fi.ModTime()}
				return
			}
			s = resolveLoc(s)
			if fi, e = os.Stat(s); e != nil {
				return fmt.Errorf("cannot access settings %q: %v", s, e)
			} else if b, e = os.ReadFile(s); e != nil {
				return fmt.Errorf("cannot read settings %q: %v", s, e)
			}
			new = &MonSettings{loc: s, ltime: fi.ModTime()}
			return
		}(); err != nil || b == nil {
			return
		} else if err = json.Unmarshal(b, new); err != nil {
			return false, fmt.Errorf("%q settings format problem: %v", new.loc, err)
		} else if s == "" && !new.Autoload {
			return false, fmt.Errorf("skipped update to %q", new.loc)
		} else if err = json.Compact(&bb, b); err != nil {
			return false, fmt.Errorf("%q settings format problem: %v", new.loc, err)
		}
		bb.WriteByte('\n')
		new.JSON = bb.String()

	case func(*MonSettings) bool: // modify settings via non-blocking function source
		mutex.Lock()
		defer mutex.Unlock()
		if cur == nil || *cur == nil || (**cur).JSON == "" || s == nil {
			return false, fmt.Errorf("no settings specified")
		} else if new = (&MonSettings{loc: (**cur).loc, ltime: (**cur).ltime}); false {
		} else if err = json.Unmarshal([]byte((**cur).JSON), new); err != nil {
			return false, fmt.Errorf("settings corrupted: %v", err)
		} else if !s(new) {
			return
		} else if b, err = json.Marshal(new); err != nil {
			return false, fmt.Errorf("cannot cache modified settings: %v", err)
		}
		new.JSON = string(append(b, '\n'))

	default:
		return false, fmt.Errorf("unknown settings source")
	}

	for _, t := range new.Variance.Templates { // finish/clean variance template map
		for _, e := range t.Envs {
			for rref, mms := range e.EC2 {
				e.EC2[rref] = getMM(mms)
			}
			e.Nre, _ = regexp.Compile(strings.Join(e.Filters["name"], ""))
		}
		for rref, mms := range t.EC2 {
			t.EC2[rref] = getMM(mms)
		}
	}
	for _, i := range new.Variance.EC2 { // finish/clean variance EC2 resource map
		switch i.Mre, _ = regexp.Compile(i.Match); i.Plat {
		case "linux", "Linux":
			i.Plat = ""
		case "Windows":
			i.Plat = "windows"
		case "sqlserver", "SQLserver":
			i.Plat = "sqlserver-se"
		}
		for _, v := range i.Vols {
			switch v.GiB = getMM(v.GiB); v.SType {
			case "sc1", "st1", "standard", "gp2", "io1", "io2":
			default:
				v.SType = "gp3"
			}
		}
	}

	new.AWS.tcmap = make(map[string]map[string]map[string][2]string) // build tag content map from TagRules to speed lookups
	new.AWS.tpmap = make(map[string]map[string][]*regexp.Regexp)     // build tag parser map from conventions in TagRules
	for k, v := range new.AWS.TagRules {
		if k != "" && k[0] != '~' {
			tc := make(map[string]map[string][2]string) // build content maps from cmon tag entries in a TagRules ruleset
			for k, v := range v {
				if strings.HasPrefix(k, "cmon:") {
					m := make(map[string][2]string)
					for k, v := range v {
						if k == "" || k[0] != '~' {
							for _, v := range v {
								m[strings.ToLower(v)] = [2]string{k, v}
							}
						}
					}
					if len(m) > 0 {
						tc[k] = m
					}
				}
			}
			if len(tc) > 0 {
				new.AWS.tcmap[k] = tc
			}

			if c := v["~conventions"]; c != nil { // build tag parsers from naming convention regexes in a TagRules ruleset
				tp := make(map[string][]*regexp.Regexp)
				for k, v := range c {
					p := []*regexp.Regexp{}
					for _, e := range v {
						if re, _ := regexp.Compile(e); re != nil {
							p = append(p, re)
						}
					}
					if len(p) > 0 {
						tp[k] = p
					}
				}
				if len(tp) > 0 {
					new.AWS.tpmap[k] = tp
				}
			}
		}
	}

	*cur, loaded = new, true // TODO: assumes atomicity of pointer assignment; consider using atomic.Value()
	return
}

// PromoteAZ method on MonSettings ...
func (s *MonSettings) PromoteAZ(acct, az string) bool {
	if s == nil {
		return false
	} else if p := s.AWS.Profiles[s.AWS.Accounts[acct]["~profile"]]; p == nil {
		return false
	} else if r := func() string {
		if len(az) > 1 && az[len(az)-1] > '9' {
			return az[:len(az)-1]
		}
		return az
	}(); p[r] == 1 {
		return false
	} else if p[r] > 0 {
		p[r] = 1
		return true
	}
	return false
}

// Update method on TagMap adds tags not in t from u
func (t TagMap) Update(u TagMap) TagMap {
	if t == nil {
		t = make(TagMap)
	}
	for k, v := range u {
		if v != "" && k != "" && k[0] != '~' && t[k] == "" {
			t[k] = v
		}
	}
	return t
}

// Update method on TagMap adds tags not in t from r (reference pointer)
func (t TagMap) UpdateR(r *TagMap) TagMap {
	if r == nil {
		return t
	}
	return t.Update(*r)
}

// UpdateP method on TagMap adds tags not in t from u having prefix p
func (t TagMap) UpdateP(u TagMap, p string) TagMap {
	if p == "" {
		return t.Update(u)
	} else if t == nil {
		t = make(TagMap)
	}
	for k, v := range u {
		if v != "" && strings.HasPrefix(k, p) && t[k] == "" {
			t[k] = v
		}
	}
	return t
}

// UpdateT method on TagMap adds tag k if not already in t
func (t TagMap) UpdateT(k, v string) TagMap {
	if t == nil {
		t = make(TagMap)
	}
	if v != "" && k != "" && t[k] == "" {
		t[k] = v
	}
	return t
}

// UpdateV method on TagMap updates tag values per content maps in TagRules settings for account a
func (t TagMap) UpdateV(s *MonSettings, a string) TagMap {
	var tr string
	var defined bool
	if t == nil || s == nil {
		return t
	} else if tr, defined = s.AWS.Accounts[a]["~tagrules"]; !defined {
		tr = "default"
	}
	if tc := s.AWS.tcmap[tr]; tc != nil {
		for k, v := range t {
			if m := tc[k]; m != nil {
				mapv, chain := func(l, v string) (string, bool) {
					if mv, ok := m[l]; ok {
						switch {
						case mv[0] == "*": // map to lookup value (rule casing if available)
							if mv[1] == "*" {
								return v, true
							}
							return mv[1], true
						case mv[0] != "" && mv[0][0] == '=':
							switch sv := strings.SplitN(mv[0][1:], "*", 2); len(sv) {
							case 1:
							default: // map to expression inserting lookup value (rule casing if available)
								if mv[1] == "*" {
									return sv[0] + v + sv[1], true
								}
								return sv[0] + mv[1] + sv[1], true
							}
						}
						return mv[0], true // simple lookup value mapping
					}
					return "", false
				}, 0
				for chain < 4 {
					if mv, ok := mapv(strings.ToLower(v), v); !ok {
					} else if chain, v, ok = chain+1, mv, !strings.EqualFold(v, mv); ok {
						continue
					}
					break
				}
				if chain > 0 {
					t[k] = v
				} else if v == "" {
				} else if mv, ok := mapv("*", v); ok {
					t[k] = mv
				}
			}
		}
	}
	return t
}

// UpdateN method on TagMap adds tags not in t from name parsed by convention c in TagRules settings for account a
func (t TagMap) UpdateN(s *MonSettings, a, c, name string) TagMap {
	var tr string
	var defined bool
	if t == nil {
		t = make(TagMap)
	}
	if s == nil || name == "" {
		return t
	} else if tr, defined = s.AWS.Accounts[a]["~tagrules"]; !defined {
		tr = "default"
	}
	for _, p := range s.AWS.tpmap[tr][c] {
		if mv := p.FindStringSubmatch(name); mv != nil {
			for i, k := range p.SubexpNames()[1:] {
				if k = "cmon:" + k; t[k] == "" {
					t[k] = mv[i+1]
				}
			}
			break
		}
	}
	return t
}
