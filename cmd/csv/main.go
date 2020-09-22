package main

import (
	"flag"
	"fmt"
	"io"
	"math"
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

func csvWriter(res *csv.Resource, heads []string) func(map[string]string) {
	var heading bool
	if heads == nil {
		heads = res.Heads
	}
	return func(row map[string]string) {
		if !heading {
			fmt.Println(`"` + strings.Join(heads, `","`) + `"`)
			heading = true
		}
		var col []string
		for _, h := range heads {
			col = append(col, strings.Replace(row[h], `"`, `""`, -1))
		}
		fmt.Println(`"` + strings.Join(col, `","`) + `"`)
	}
}

func getRes(scache *csv.Settings, fn string) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("%v\n", e)
		}
		wg.Done()
	}()
	var (
		r       io.ReadCloser
		decoder tel.Decoder
		rater   tel.Rater
		write   func(map[string]string)
		tn      tel.E164full
		rfmt    string
	)
	if fn == "" {
		fn, r = "<stdin>", os.Stdin
	}

	res, rows := csv.Resource{Location: fn, Comment: "#", Shebang: "#!", SettingsCache: scache}, 0
	if e := res.Open(r); e != nil {
		panic(fmt.Errorf("error opening %q: %v", fn, e))
	}
	defer res.Close()
	if csvFlag {
		write = csvWriter(&res, nil)
	} else if rateFlag {
		switch rfmt = res.Settings.Format; rfmt {
		case "Intelepeer CDR":
			rater.Default = tel.T1intlTermRates
			write = csvWriter(&res, []string{
				"Call Date",
				"Call Time",
				"Customer Account ID",
				"To Country Code",
				"Terminating Phone Number",
				"Route",
				"Billable Time",
				"Rate Per Minute",
				"Billable Amount",
				"Unique CDR ID",
				"Re-rated Amount",
			})
		case "Aspect CDR":
			write = csvWriter(&res, []string{
				"gatewayAccountingId",
				"startTime",
				"endTime",
				"fromNumber",
				"toNumber",
				"ISO3166",
				"callDirection",
				"rawDuration",
				"meteredDuration",
				"charges",
				"reratedCharges",
			})
		}
		if e := decoder.Load(nil); e != nil {
			panic(e)
		} else if e := rater.Load(nil); e != nil {
			panic(e)
		}
	}
	res.Cols = updateSettings(&res, colsFlag, forceFlag)
	in, err := res.Get()

	filtered, failed, charged, rated, ch, ra := 0, 0, 0.0, 0.0, 0.0, 0.0
	for row := range in {
		if rows++; csvFlag {
			write(row)
		} else if debugFlag {
			fmt.Println(row)
		} else if rateFlag {
			switch filtered++; rfmt {
			case "Intelepeer CDR":
				if err := decoder.Full(row["To Country Code"]+row["Terminating Phone Number"], &tn); err != nil {
					failed++
					continue
				}
				d, _ := strconv.ParseFloat(row["Billable Time"], 64)
				ch, _ = strconv.ParseFloat(row["Billable Amount"], 64)
				if ra = float64(rater.Lookup(&tn)) * d; ra == 0 || ch-ra < 0.1 {
					ra = ch
				} else {
					ra = math.Round(ra*1e4) / 1e4
				}
				rated += ra
				charged += ch
				row["Re-rated Amount"] = fmt.Sprintf("%.4f", ra)
				write(row)
			case "Aspect CDR":
				if err := decoder.Full(row["toNumber"], &tn); err != nil {
					failed++
					continue
				}
				d, _ := strconv.ParseFloat(row["rawDuration"], 64)
				d /= 60000
				ch, _ := strconv.ParseFloat(row["charges"], 64)
				ra := float64(rater.Lookup(&tn)) * d
				charged += ch
				rated += ra
				row["ISO3166"], row["reratedCharges"] = tn.ISO3166, fmt.Sprintf("%.3f", ra)
				write(row)
			}
		}
	}
	if e := <-err; e != nil {
		panic(fmt.Errorf("error reading %q: %v", fn, e))
	} else if rateFlag {
		fmt.Printf("filtered %d %q records (%d failed); $%.2f charged -- rerated to $%.2f\n",
			filtered, rfmt, failed, charged, rated)
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
