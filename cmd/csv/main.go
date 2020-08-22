package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/tel"
)

var (
	settingsFlag string
	csvFlag      bool
	debugFlag    bool
	rateFlag     bool
	forceFlag    bool
	heading      bool
	colsFlag     string
	wg           sync.WaitGroup
)

func init() {
	// set up command-line flags
	flag.StringVar(&settingsFlag, "s", "~/.csv_settings.json", fmt.Sprintf("file-type settings `file` containing column filter maps"))
	flag.BoolVar(&forceFlag, "f", false, fmt.Sprintf("force file-type settings to settings file"))
	flag.BoolVar(&csvFlag, "c", false, fmt.Sprintf("specify CSV output"))
	flag.BoolVar(&debugFlag, "d", false, fmt.Sprintf("specify debug output"))
	flag.BoolVar(&rateFlag, "r", false, fmt.Sprintf("specify call rating output"))
	flag.StringVar(&colsFlag, "cols", "", fmt.Sprintf("column filter `map`: "+
		"'[!]<head>[:(=|!){<pfx>[:<pfx>]...}][[:<bcol>]:<col>][,...]'  (ex. 'name,,!stat:={OK},age,acct:!{n/a:0000}:6')"))

	// call on ErrHelp
	flag.Usage = func() {
		fmt.Printf("command usage: csv [-c] [-d] [-f] [-cols '<map>'] [-s <file>] <csvfile> [...]" +
			"\n\nThis command identifies and parses CSV and fixed-field TXT files using column filter maps.\n\n")
		flag.PrintDefaults()
	}
}

func updateSettings(res *csv.Resource, cflag string, force bool) (cols string) {
	switch cflag {
	case "":
		cols = res.Settings.Cols
	case "*":
	default:
		if cols = cflag; res.Settings.Cols != cols {
			res.Settings.Cols, res.Settings.Date = cols, time.Now()
		}
	}

	switch res.Typ { // these initial settings should be manually updated
	case csv.RTcsv:
		if res.Sig == "" && force {
			res.Sig, res.Settings.Date = fmt.Sprintf("=%s%d", string(res.Sep), len(res.Split[0])), time.Now()
		}
		if res.Settings.Format == "" && res.Settings.Ver == "" {
			res.Settings.Format, res.Settings.Ver, res.Settings.Date = "unspecified CSV", res.Location, time.Now()
		}
	case csv.RTfixed:
		if res.Sig == "" && force {
			res.Sig, res.Settings.Date = fmt.Sprintf("=h%d,f%d", len(res.Preview[0]), len(res.Preview[1])), time.Now()
		}
		if res.Settings.Format == "" && res.Settings.Ver == "" {
			res.Settings.Format, res.Settings.Ver, res.Settings.Date = "unspecified fixed-field", res.Location, time.Now()
		}
	}
	if force || !res.Settings.Lock && res.Settings.Cols != "" {
		res.SettingsCache.Set(res.Sig, &res.Settings)
	}
	return
}

func writeCSV(res *csv.Resource, row map[string]string) {
	if !heading {
		fmt.Println(`"` + strings.Join(res.Heads, `","`) + `"`)
		heading = true
	}
	var col []string
	for _, h := range res.Heads {
		col = append(col, strings.Replace(row[h], `"`, `""`, -1))
	}
	fmt.Println(`"` + strings.Join(col, `","`) + `"`)
}

func getRes(scache *csv.Settings, fn string) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("%v\n", e)
		}
		wg.Done()
	}()
	var (
		r       io.Reader
		decoder tel.E164
		rater   tel.Rate
	)
	if fn == "" {
		fn, r = "<stdin>", os.Stdin
	}
	if rateFlag {
		if e := decoder.Load(nil); e != nil {
			panic(fmt.Errorf("error loading E.164 decoder: %v", e))
		} else if e := rater.Load(nil); e != nil {
			panic(fmt.Errorf("error loading telephony rater: %v", e))
		}
	}

	res, rows := csv.Resource{Location: fn, Comment: "#", Shebang: "#!", SettingsCache: scache}, 0
	if e := res.Open(r); e != nil {
		panic(fmt.Errorf("error opening %q: %v", fn, e))
	}
	defer res.Close()
	res.Cols = updateSettings(&res, colsFlag, forceFlag)
	in, err := res.Get()

	filtered, failed, charged, rated := 0, 0, 0.0, 0.0
	for row := range in {
		if rows++; csvFlag {
			writeCSV(&res, row)
		} else if rateFlag && row["callDirection"] == "PSTN_OUTBOUND" {
			filtered++
			if n := decoder.Decode(row["toNumber"]); n == "" {
				failed++
				fmt.Println(row)
				continue
			}
			d, _ := strconv.ParseFloat(row["rawDuration"], 32)
			d /= 60000
			c, _ := strconv.ParseFloat(row["charges"], 32)
			r := float64(rater.Lookup(&decoder)) * d
			charged += c
			rated += r
			fmt.Printf("re-rated %.1fm call to +%v (+%v %v) at $%.3f (billed at $%.3f)\n",
				d, decoder.Num, decoder.CC, decoder.ISO3166, r, c)
		} else if debugFlag {
			fmt.Println(row)
		}
	}
	if e := <-err; e != nil {
		panic(fmt.Errorf("error reading %q: %v", fn, e))
	} else if rateFlag {
		fmt.Printf("filtered %d records (%d failed); $%.2f charged -- rerated to $%.2f\n",
			filtered, failed, charged, rated)
	} else if !csvFlag {
		fmt.Printf("read %d rows from [%s %s] resource at %q\n", rows, res.Settings.Format, res.Settings.Ver, fn)
	}
}

func main() {
	flag.Parse()
	settings := csv.Settings{Location: settingsFlag}
	defer settings.Sync()
	settings.Cache(nil)

	if len(flag.Args()) > 0 {
		for _, arg := range flag.Args() {
			files, _ := filepath.Glob(arg)
			if len(files) == 0 {
				files = []string{arg}
			}
			for _, file := range files {
				wg.Add(1)
				go getRes(&settings, file)
			}
		}
	} else {
		wg.Add(1)
		go getRes(&settings, "")
	}
	wg.Wait()
}
