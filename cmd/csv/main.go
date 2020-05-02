package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/sententico/cost/csv"
)

var (
	settingsFlag string
	detailFlag   bool
	colsFlag     string
	fcolsFlag    string
	config       csv.Settings
	wg           sync.WaitGroup
)

func init() { // set up command-line flags
	flag.StringVar(&settingsFlag, "s", ".csv_settings.json", fmt.Sprintf("specify settings `file`"))
	flag.BoolVar(&detailFlag, "d", false, fmt.Sprintf("specify detailed output"))
	flag.StringVar(&colsFlag, "cols", "", fmt.Sprintf("specify CSV column `map`, like \"name:2,age:5\""))
	flag.StringVar(&fcolsFlag, "fcols", "", fmt.Sprintf("specify fixed-column `map`, like \"name:10:35,age:40:42\""))

	flag.Usage = func() {
		fmt.Printf("command usage: gt <flags>\n")
		flag.PrintDefaults()
	}
}

func peekOpen(path string, dig *csv.Digest) (<-chan map[string]string, <-chan error, chan<- int) {
	var e error
	switch *dig, e = csv.Peek(path); {
	case e != nil:
		panic(fmt.Errorf("%v", e))

	case dig.Sep == '\x00':
		// return open fixed-field TXT reader channels
		switch {
		case fcolsFlag == "":
			fcolsFlag = dig.Cache.Cols
		case dig.Sig != "" && !dig.Cache.Lock:
			dig.Cache.Cols = fcolsFlag
			dig.Cache.Date = time.Now()
		}
		return csv.ReadFixed(path, fcolsFlag, dig.Comment)
	default:
		// update column map cache & return open CSV reader channels
		switch {
		case colsFlag == "":
			colsFlag = dig.Cache.Cols
		case colsFlag == "*":
			colsFlag = ""
		case dig.Sig != "" && !dig.Cache.Lock:
			dig.Cache.Cols = colsFlag
			dig.Cache.Date = time.Now()
			config[dig.Sig] = dig.Cache
		}
		return csv.Read(path, colsFlag, dig.Comment, dig.Sep)
	}
}

func main() {
	flag.Parse()
	config = csv.GetConfig(settingsFlag)
	defer csv.SetConfig()
	for _, file := range flag.Args() {
		wg.Add(1)

		go func(f string) { // one go routine started per file
			defer func() {
				if e := recover(); e != nil {
					fmt.Printf("%v\n", e)
				}
				defer wg.Done()
			}()
			var dig csv.Digest
			in, err, sig := peekOpen(f, &dig)
			defer close(sig)

			rows := 0
			for row := range in {
				if rows++; detailFlag {
					fmt.Println(row)
				}
			}
			if e := <-err; e != nil {
				panic(fmt.Errorf("%v", e))
			}
			fmt.Printf("read %d rows from [%s] file %q\n", rows, dig.Cache.Type, f)
		}(file)
	}
	wg.Wait()
}
