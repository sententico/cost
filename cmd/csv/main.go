package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/sententico/cost/csv"
)

var (
	settingsFlag string
	detailFlag   bool
	forceFlag    bool
	colsFlag     string
	wg           sync.WaitGroup
)

func init() {
	// set up command-line flags
	flag.StringVar(&settingsFlag, "s", "~/.csv_settings.json", fmt.Sprintf("file-type settings `file` containing column filter maps"))
	flag.BoolVar(&forceFlag, "f", false, fmt.Sprintf("force file-type settings to settings file"))
	flag.BoolVar(&detailFlag, "d", false, fmt.Sprintf("specify detailed output"))
	flag.StringVar(&colsFlag, "cols", "", fmt.Sprintf("column filter `map`: "+
		"'[!]<head>[:(=|!){<pfx>[:<pfx>]...}][[:<bcol>]:<col>][,...]'  (ex. 'name,,!stat:={OK},age,acct:!{n/a:0000}:6')"))

	// call on ErrHelp
	flag.Usage = func() {
		fmt.Printf("command usage: csv [-d] [-f] [-cols '<map>'] [-s <file>] <csvfile> [...]" +
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
		if force && res.Sig == "" {
			res.Sig, res.Settings.Date = fmt.Sprintf("=%s%d", string(res.Sep), len(res.Split[0])), time.Now()
		}
		if res.Settings.Format == "" && res.Settings.Ver == "" {
			res.Settings.Format, res.Settings.Ver, res.Settings.Date = "unspecified CSV", res.Name, time.Now()
		}
	case csv.RTfixed:
		if force && res.Sig == "" {
			res.Sig, res.Settings.Date = fmt.Sprintf("=h%d,f%d", len(res.Preview[0]), len(res.Preview[1])), time.Now()
		}
		if res.Settings.Format == "" && res.Settings.Ver == "" {
			res.Settings.Format, res.Settings.Ver, res.Settings.Date = "unspecified fixed-field", res.Name, time.Now()
		}
	}
	if force || !res.Settings.Lock && res.Settings.Cols != "" {
		res.SettingsCache.Set(res.Sig, &res.Settings)
	}
	return
}

func main() {
	flag.Parse()
	settings := csv.Settings{Name: settingsFlag}
	defer settings.Sync()
	settings.Cache(nil)

	for _, arg := range flag.Args() {
		files, _ := filepath.Glob(arg)
		if len(files) == 0 {
			files = []string{arg}
		}
		for _, file := range files {
			wg.Add(1)

			go func(f string) { // one go routine started per file
				defer func() {
					if e := recover(); e != nil {
						fmt.Printf("%v\n", e)
					}
					wg.Done()
				}()
				res, rows := csv.Resource{Name: f, SettingsCache: &settings}, 0
				if e := res.Open(nil); e != nil {
					panic(fmt.Errorf("error opening %q: %v", f, e))
				}
				defer res.Close()
				updateSettings(&res, colsFlag, forceFlag)
				in, err := res.Get()

				for row := range in {
					if rows++; detailFlag {
						fmt.Println(row)
					}
				}
				if e := <-err; e != nil {
					panic(fmt.Errorf("error reading %q: %v", f, e))
				}
				fmt.Printf("read %d rows from [%s %s] file %q\n", rows, res.Settings.Format, res.Settings.Ver, f)
			}(file)
		}
	}
	wg.Wait()
}
