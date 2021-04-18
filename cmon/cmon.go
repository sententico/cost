package cmon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Cloud Monitor settings types
type (
	// TagMap ...
	TagMap map[string]string

	// alertsFeature settings
	alertsFeature struct {
		Options             string
		Profiles, Customers map[string]map[string]string
	}

	// awsService settings
	awsService struct {
		Options                    string
		SavPlan                    string
		SavCov, SpotDisc, UsageAdj float32
		SES                        map[string]string
		Profiles                   map[string]map[string]float32
		Regions, Accounts          map[string]TagMap
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

// Reload Cloud Monitor settings from location or function source
func Reload(cur **MonSettings, source interface{}) (err error) {
	var new *MonSettings
	var b []byte
	mutex.Lock()
	defer mutex.Unlock()

	switch s := source.(type) {
	case string: // (re)load settings from location (file, ...) source
		var bb bytes.Buffer
		if cur == nil || *cur == nil && s == "" {
			return fmt.Errorf("no settings specified")
		} else if err = func() (e error) {
			var fi fs.FileInfo
			if s == "" {
				if fi, _ = os.Stat((**cur).loc); fi == nil || fi.ModTime() == (**cur).ltime {
					return // no reloadable update available
				} else if (**cur).ltime = fi.ModTime(); false { // treat as if loaded
				} else if b, e = os.ReadFile((**cur).loc); e != nil {
					return fmt.Errorf("cannot read settings %q: %v", (**cur).loc, e)
				}
				new = &MonSettings{loc: (**cur).loc, ltime: (**cur).ltime}
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
			return fmt.Errorf("%q settings format problem: %v", new.loc, err)
		} else if s == "" && !new.Autoload {
			return fmt.Errorf("skipped update to %q", new.loc)
		} else if err = json.Compact(&bb, b); err != nil {
			return fmt.Errorf("%q settings format problem: %v", new.loc, err)
		}
		bb.WriteByte('\n')
		new.JSON = bb.String()

	case func(*MonSettings) bool: // modify settings via function source
		if cur == nil || *cur == nil || (**cur).JSON == "" || s == nil {
			return fmt.Errorf("no settings specified")
		} else if new = (&MonSettings{loc: (**cur).loc, ltime: (**cur).ltime}); false {
		} else if err = json.Unmarshal([]byte((**cur).JSON), new); err != nil {
			return fmt.Errorf("settings corrupted: %v", err)
		} else if !s(new) {
			return
		} else if b, err = json.Marshal(new); err != nil {
			return fmt.Errorf("cannot cache modified settings: %v", err)
		}
		new.JSON = string(append(b, '\n'))

	default:
		return fmt.Errorf("unknown settings source")
	}
	*cur = new
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

// Update method on TagMap ...
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

// UpdateT method on TagMap ...
func (t TagMap) UpdateT(k, v string) TagMap {
	if t == nil {
		t = make(TagMap)
	}
	if v != "" && k != "" && k[0] != '~' && t[k] == "" {
		t[k] = v
	}
	return t
}
