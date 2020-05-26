package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/sententico/cost/agg"
	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/flt"
	_ "github.com/sententico/cost/internal/pfax"
	"github.com/sententico/cost/xfm"
)

type fentry struct {
	cols string
	flt  func(chan<- interface{}, <-chan map[string]string)
}
type fmap map[string]fentry

var (
	settingsFlag string
	wg           sync.WaitGroup
	faxMap       = map[string]struct {
		descr string
		xfm   func(interface{})
		agg   func(<-chan interface{}) interface{}
		fm    fmap
	}{
		"wc": {`wc transform desciption`, xfm.WC, agg.WC, fmap{"*": {"", flt.WC}}},
	}
)

func init() {
	// set up command-line flags
	flag.StringVar(&settingsFlag, "s", "~/.csv_settings.json", fmt.Sprintf("file-type settings `file` containing column filter maps"))

	// call on ErrHelp
	flag.Usage = func() {
		fmt.Printf("command usage: pfax [-s <file>] <csvfile> [...]" +
			"\n\nThis command...\n\n")
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	csv.Settings.Cache(settingsFlag)
	x := faxMap["wc"]

	fin := make(chan interface{}, 64)
	for _, arg := range flag.Args() {
		files, _ := filepath.Glob(arg)
		if len(files) == 0 {
			files = []string{arg}
		}
		for _, file := range files {
			wg.Add(1)

			go func(fn string) {
				defer func() {
					if e := recover(); e != nil {
						fmt.Printf("%v\n", e)
					}
					wg.Done()
				}()
				var (
					dig  csv.Digest
					in   <-chan map[string]string
					err  <-chan error
					sig  chan<- int
					e    error
					fe   fentry
					ok   bool
					cols string
				)
				if dig, e = csv.Peek(fn); e != nil {
					panic(fmt.Errorf("%v", e))
				} else if fe, ok = x.fm[dig.Settings.Type]; !ok {
					if fe, ok = x.fm["*"]; !ok {
						panic(fmt.Errorf("no filter defined for %q [%v]", fn, dig.Settings.Type))
					}
				}
				if cols = fe.cols; cols == "" {
					cols = dig.Settings.Cols
				}
				if dig.Sep == '\x00' {
					in, err, sig = csv.ReadFixed(fn, cols, dig.Comment, dig.Heading)
				} else {
					in, err, sig = csv.Read(fn, cols, dig.Comment, dig.Heading, dig.Sep)
				}
				defer close(sig)

				fe.flt(fin, in)
				if e := <-err; e != nil {
					panic(fmt.Errorf("%v", e))
				}
			}(file)
		}
	}
	go func() {
		defer close(fin)
		wg.Wait()
	}()
	x.xfm(x.agg(fin))
}
