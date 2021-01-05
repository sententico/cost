package cmon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// Cloud Monitor settings types
type (
	// awsService settings
	awsService struct {
		Options                    string
		SavPlan                    string
		SavCov, SpotDisc, UsageAdj float32
		Profiles                   map[string]map[string]float32
		Regions, Accounts          map[string]map[string]string
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
		Options         string
		Unit, Address   string
		WorkDir, BinDir string
		Models          map[string]string
		AWS             awsService
		Datadog         datadogService
		Slack           slackService
		JSON            string `json:"-"`
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

	// StreamArgs ...
	StreamArgs struct {
		Token string // Admin.Auth access token (renew hourly to avoid expiration)
		Model string // model name
		Items int    // maximum items
	}

	// StreamCURArgs ...
	StreamCURArgs struct {
		Token    string  // Admin.Auth access token (renew hourly to avoid expiration)
		From     int32   // from hour
		To       int32   // to hour
		Units    int16   // item reporting units (hour=1, day=24, month=720)
		Items    int     // maximum items
		Truncate float64 // filter items with costs below this in absolute value
	}
)

// Getarg helper function ...
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

// Load method on MonSettings ...
func (s *MonSettings) Load(loc string) (err error) {
	var b []byte
	var bb bytes.Buffer
	if s == nil || loc == "" {
		return fmt.Errorf("no settings specified")
	} else if b, err = ioutil.ReadFile(loc); err != nil {
		// TODO: work up directory hierarchy or check home directory?
		return fmt.Errorf("cannot access settings %q: %v", loc, err)
	} else if err = json.Unmarshal(b, s); err != nil {
		return fmt.Errorf("%q settings format problem: %v", loc, err)
	} else if err = json.Compact(&bb, b); err != nil {
		return fmt.Errorf("%q settings format problem: %v", loc, err)
	} else {
		bb.WriteByte('\n')
		s.JSON = bb.String()
	}
	return nil
}
