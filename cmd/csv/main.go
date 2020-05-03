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
	wg           sync.WaitGroup
)

func init() { // set up command-line flags
	flag.StringVar(&settingsFlag, "s", ".csv_settings.json", fmt.Sprintf("file-type settings `file`"))
	flag.BoolVar(&detailFlag, "d", false, fmt.Sprintf("specify detailed output"))
	flag.StringVar(&colsFlag, "cols", "", fmt.Sprintf("CSV column `map`, like...   "+
		"\"name:2,age:5\",\t\t"+
		"(\"(<cnam>[:<col>])...\")"))
	flag.StringVar(&fcolsFlag, "fcols", "", fmt.Sprintf("fixed-column `map`, like... "+
		"\"name:20,~:39,age:42\",\t"+
		"(\"{{<cnam>|~}:<ecol> | <cnam>:<bcol>:<ecol>}...[<cnam>]\")"))
	flag.Usage = func() {
		fmt.Printf("command usage: csv [-<flags>]... <csvfile>...\n")
		flag.PrintDefaults()
	}
}

func peekOpen(path string, dig *csv.Digest) (<-chan map[string]string, <-chan error, chan<- int) {
	var e error
	switch *dig, e = csv.Peek(path); {
	case e != nil:
		panic(fmt.Errorf("%v", e))

	case dig.Sep == '\x00':
		// update cached column map (if specified) & return fixed-field TXT reader channels
		switch {
		case fcolsFlag == "":
			fcolsFlag = dig.Settings.Cols
		case dig.Sig != "" && !dig.Settings.Lock:
			dig.Settings.Cols = fcolsFlag
			dig.Settings.Date = time.Now()
			csv.Settings.Set(dig.Sig, dig.Settings)
		}
		return csv.ReadFixed(path, fcolsFlag, dig.Comment)
	default:
		// update cached column map (if specified) & return CSV reader channels
		switch {
		case colsFlag == "":
			colsFlag = dig.Settings.Cols
		case colsFlag == "*":
			colsFlag = ""
		case dig.Sig != "" && !dig.Settings.Lock:
			dig.Settings.Cols = colsFlag
			dig.Settings.Date = time.Now()
			csv.Settings.Set(dig.Sig, dig.Settings)
		}
		return csv.Read(path, colsFlag, dig.Comment, dig.Sep)
	}
}

func main() {
	flag.Parse()
	csv.Settings.Cache(settingsFlag)
	defer csv.Settings.Write()
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
			fmt.Printf("read %d rows from [%s] file %q\n", rows, dig.Settings.Type, f)
		}(file)
	}
	wg.Wait()
}
