package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/sententico/cost/agg"
	"github.com/sententico/cost/csv"
	"github.com/sententico/cost/flt"
	"github.com/sententico/cost/internal/pfax"
	"github.com/sententico/cost/xfm"
)

var wg sync.WaitGroup

func init() {
	// set up command-line flags
	pfax.Args.XfmFlag = "wc"
	flag.Var(&pfax.Args.XfmFlag, "x", fmt.Sprintf("transform `xfm` to be applied to CSV/fixed-field files"))
	flag.StringVar(&pfax.Args.SettingsFlag, "s", "~/.csv_settings.json", fmt.Sprintf("file-type settings `file` containing column filter maps"))

	// call on ErrHelp
	flag.Usage = func() {
		fmt.Printf("command usage: pfax [-s <file>] <csvfile> [...]" +
			"\n\nThis command...\n\n")
		flag.PrintDefaults()
	}

	pfax.Xm = pfax.Xmap{
		"wc": {`wc transform desciption`, xfm.WC, agg.WC, pfax.Fmap{
			"Level 3 CDR": {flt.WC, "SERVTYPE,!BILL_IND:!{N},BILLINGNUM,DESTYPEUSED"},
			"*":           {flt.WC, ""},
		}},
	}
}

func main() {
	flag.Parse()
	settings := csv.Settings{Location: pfax.Args.SettingsFlag}
	settings.Cache(nil)
	x, fin := pfax.Xm[string(pfax.Args.XfmFlag)], make(chan interface{}, 64)

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
					res = csv.Resource{Location: fn, SettingsCache: &settings}
					e   error
					fe  pfax.Fentry
					ok  bool
					in  <-chan map[string]string
					err <-chan error
				)
				if e = res.Open(nil); e != nil {
					panic(fmt.Errorf("error opening %q: %v", fn, e))
				}
				defer res.Close()
				if fe, ok = x.Fm[res.Settings.Format]; !ok {
					if fe, ok = x.Fm["*"]; !ok {
						panic(fmt.Errorf("no filter defined for %q [%v]", fn, res.Settings.Format))
					}
				}
				if fe.Cols != "" {
					res.Cols = fe.Cols
				}
				in, err = res.Get()

				fe.Flt(fin, in, res)
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
	x.Xfm(x.Agg(fin))
}
