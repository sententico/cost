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
	fcolsFlag    string
	wg           sync.WaitGroup
)

func init() {
	// set up command-line flags
	flag.StringVar(&settingsFlag, "s", "~/.csv_settings.json", fmt.Sprintf("file-type settings `file` containing column maps"))
	flag.BoolVar(&forceFlag, "f", false, fmt.Sprintf("force file-type settings to settings file"))
	flag.BoolVar(&detailFlag, "d", false, fmt.Sprintf("specify detailed output"))
	flag.StringVar(&colsFlag, "cols", "", fmt.Sprintf("CSV column `map`, like...   "+
		"'name:2,age:5',\t\t"+
		"('<head>[:<col>][,<head>[:<col>]]...')"))
	flag.StringVar(&fcolsFlag, "fcols", "", fmt.Sprintf("fixed-column `map`, like... "+
		"'name:20,~:39,age:42',\t"+
		"('(<head>|~):<ecol>|<head>:<bcol>:<ecol>[,(<head>|~):<ecol>|<head>:<bcol>:<ecol>]...[,<head>]')"))

	// call on ErrHelp
	flag.Usage = func() {
		fmt.Printf("command usage: csv [-d] [-f] [-cols '<map>'] [-fcols '<map>'] [-s <file>] <csvfile> [<csvfile> ...]" +
			"\n\nThis command identifies and parses CSV and fixed-field TXT files using column selection maps\n\n")
		flag.PrintDefaults()
	}
}

func updateSettings(dig *csv.Digest, path, cflag string, force bool) (cols string) {
	switch cflag {
	case "":
		cols = dig.Settings.Cols
	case "*":
	default:
		if cols = cflag; dig.Settings.Cols != cols {
			dig.Settings.Cols, dig.Settings.Date = cols, time.Now()
		}
	}

	switch dig.Sep { // these initial settings should be manually updated
	case '\x00':
		if force && dig.Sig == "" {
			dig.Sig, dig.Settings.Date = fmt.Sprintf("=h%d,f%d", len(dig.Preview[0]), len(dig.Preview[1])), time.Now()
		}
		if dig.Settings.Type == "" && dig.Settings.Ver == "" {
			dig.Settings.Type, dig.Settings.Ver, dig.Settings.Date = "unspecified fixed-field", path, time.Now()
		}
	default:
		if force && dig.Sig == "" {
			dig.Sig, dig.Settings.Date = fmt.Sprintf("=%s%d", string(dig.Sep), len(dig.Split[0])), time.Now()
		}
		if dig.Settings.Type == "" && dig.Settings.Ver == "" {
			dig.Settings.Type, dig.Settings.Ver, dig.Settings.Date = "unspecified CSV", path, time.Now()
		}
	}
	if force || !dig.Settings.Lock && dig.Settings.Cols != "" {
		csv.Settings.Set(dig.Sig, dig.Settings)
	}
	return
}

func peekOpen(path string, dig *csv.Digest) (<-chan map[string]string, <-chan error, chan<- int) {
	var e error
	if *dig, e = csv.Peek(path); e != nil {
		panic(fmt.Errorf("%v", e))
	} else if dig.Sep == '\x00' {
		return csv.ReadFixed(path, updateSettings(dig, path, fcolsFlag, forceFlag), dig.Comment, dig.Heading)
	}
	return csv.Read(path, updateSettings(dig, path, colsFlag, forceFlag), dig.Comment, dig.Heading, dig.Sep)
}

func main() {
	flag.Parse()
	csv.Settings.Cache(settingsFlag)
	defer csv.Settings.Write()
	for _, arg := range flag.Args() {
		files, _ := filepath.Glob(arg)
		for _, file := range files {
			wg.Add(1)

			go func(f string) { // one go routine started per file
				defer func() {
					if e := recover(); e != nil {
						fmt.Printf("%v\n", e)
					}
					defer wg.Done()
				}()
				dig, rows := csv.Digest{}, 0
				in, err, sig := peekOpen(f, &dig)
				defer close(sig)

				for row := range in {
					if rows++; detailFlag {
						fmt.Println(row)
					}
				}
				if e := <-err; e != nil {
					panic(fmt.Errorf("%v", e))
				}
				fmt.Printf("read %d rows from [%s] file %q\n", rows, dig.Settings.Type, f)
			}(file)
		}
	}
	wg.Wait()
}
