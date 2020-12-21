package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sententico/cost/cmon"
)

type (
	intHours struct {
		from, to int32
		units    int16
	}
)

var (
	args struct {
		settings  string   // settings location
		address   string   // cmon server address:port
		debug     bool     // debug output enabled
		more      []string // unparsed arguments
		metric    string   // series metric
		items     int      // maximum stream items
		truncate  float64  // series/stream amount truncation filter
		interval  intHours // from/to/units hours
		span      int      // series total hours
		recent    int      // series recent/active hours
		seriesSet *flag.FlagSet
		streamSet *flag.FlagSet
	}
	address  string           // cmon server address (args override settings file)
	settings cmon.MonSettings // settings
	command  string           // cmon subcommand
	wg       sync.WaitGroup   // thread synchronization
)

func init() {
	flag.StringVar(&args.settings, "s", "", "settings `file`")
	flag.StringVar(&args.address, "a", "", "cmon server location `address:port`")
	flag.BoolVar(&args.debug, "d", false, fmt.Sprintf("specify debug output"))
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nCommand usage: cmon [-s] [-a] [-d] <subcommand> [<subcommand arg> ...]"+
				"\n\nThis is the command-line interface to the Cloud Monitor. Subcommands generally map to API"+
				"\ninterfaces and access model content within the Cloud Monitor.\n\n")
		flag.PrintDefaults()
		args.seriesSet.Usage()
		args.streamSet.Usage()
		fmt.Fprintln(flag.CommandLine.Output())
	}

	args.seriesSet = flag.NewFlagSet("series", flag.ExitOnError)
	args.seriesSet.StringVar(&args.metric, "metric", "cdr.asp/term/geo/n", "series metric `type`")
	args.seriesSet.IntVar(&args.span, "span", 12, "series total `hours`")
	args.seriesSet.IntVar(&args.recent, "recent", 3, "`hours` of recent/active part of span")
	args.seriesSet.Float64Var(&args.truncate, "truncate", 0, "recent `amount` metric truncation threshold")
	args.seriesSet.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"series\" subcommand returns an hourly series for a metric type.\n")
		args.seriesSet.PrintDefaults()
	}

	args.streamSet = flag.NewFlagSet("stream", flag.ExitOnError)
	y, m, _ := time.Now().Date() // set default to prior month
	t := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	args.interval = intHours{int32(t.AddDate(0, -1, 0).Unix() / 3600), int32((t.Unix() - 1) / 3600), 720}
	args.streamSet.Var(&args.interval, "interval", "`YYYY-MM[-DD[Thh]][+r]` month/day/hour +range to stream")
	args.streamSet.IntVar(&args.items, "items", 2e5, "`maximum` items to stream")
	args.streamSet.Float64Var(&args.truncate, "truncate", 0.005, "`cost` item truncation threshold")
	args.streamSet.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"stream\" subcommand returns an item detail stream.\n")
		args.streamSet.PrintDefaults()
	}
}

func (i *intHours) String() string {
	if i != nil {
		switch fr := time.Unix(int64(i.from)*3600, 0).UTC().Format(time.RFC3339); i.units {
		case 1:
			return fmt.Sprintf("%.13s%+d", fr, i.to-i.from)
		case 24:
			return fmt.Sprintf("%.10s%+d", fr, (i.to-i.from)/24)
		case 720:
			return fmt.Sprintf("%.7s%+d", fr, (i.to-i.from+1)/672-1) // TODO: breaks @>10mo.
		}
	}
	return ""
}
func (i *intHours) Set(arg string) error {
	var err error
	var t time.Time
	var r int
	s := strings.SplitN(arg, "+", 2)
	if len(s) > 1 {
		if r, err = strconv.Atoi(s[1]); err != nil || r > 167 {
			return fmt.Errorf("invalid argument")
		}
	}
	switch len(s[0]) {
	case 7:
		if t, err = time.Parse(time.RFC3339, s[0]+"-01T00:00:00Z"); err == nil {
			i.from, i.to, i.units = int32(t.Unix())/3600, int32(t.AddDate(0, r+1, 0).Unix()-1)/3600, 720
			return nil
		}
	case 10:
		if t, err = time.Parse(time.RFC3339, s[0]+"T00:00:00Z"); err == nil {
			i.from, i.to, i.units = int32(t.Unix())/3600, int32(t.AddDate(0, 0, r+1).Unix()-1)/3600, 24
			return nil
		}
	case 13:
		if t, err = time.Parse(time.RFC3339, s[0]+":00:00Z"); err == nil {
			i.from = int32(t.Unix()) / 3600
			i.to, i.units = i.from+int32(r), 1
			return nil
		}
	}
	return fmt.Errorf("invalid argument")
}

func fatal(ex int, format string, a ...interface{}) {
	fmt.Fprintf(flag.CommandLine.Output(), "\n"+format+"\n\n", a...)
	os.Exit(ex)
}

func defaultWorker(in chan string) {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}

	var r string
	for ln := range in {
		if err = client.Call("API.Upper", ln, &r); err != nil {
			fatal(1, "error calling GoRPC: %v", err)
		}
		fmt.Printf("%v\n", r)
	}
	client.Close()
	wg.Done()
}

func defaultCmd() {
	in := make(chan string, 20)
	go func(ln *bufio.Scanner, in chan string) {
		for ; ln.Scan(); in <- ln.Text() {
		}
		close(in)
	}(bufio.NewScanner(os.Stdin), in)

	for i := 0; i < cap(in)/2; i++ {
		wg.Add(1)
		go defaultWorker(in)
	}
	wg.Wait()
}

func seriesCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r map[string][]float64
	if err = client.Call("API.Series", &cmon.SeriesArgs{
		Token:    "placeholder_access_token",
		Metric:   args.metric,
		Span:     args.span,
		Recent:   args.recent,
		Truncate: args.truncate,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	}
	client.Close()
	for k, ser := range r {
		fmt.Printf("%v: %.6g\n", k, ser)
	}
}

func streamcurCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r [][]string
	if err = client.Call("API.StreamCUR", &cmon.StreamCURArgs{
		Token:    "placeholder_access_token",
		From:     args.interval.from,
		To:       args.interval.to,
		Units:    args.interval.units,
		Items:    args.items,
		Truncate: args.truncate,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	}
	if client.Close(); len(r) > 0 {
		warn, unit := "", "Month"
		switch args.interval.units {
		case 1:
			unit = "Hour"
		case 24:
			unit = "Day"
		}
		if len(r) == args.items {
			warn = " [item limit reached]"
		}
		fmt.Printf("Invoice ID%s,%s,Account,Type,Service,Usage Type,Operation,Region,Resource ID"+
			",Item Description,Name,Env,DC,Product,App,Cust,Team,Ver,Recs,Usage,Billed\n", warn, unit)
		for _, row := range r {
			fmt.Printf("\"%s\"\n", strings.Join(row, "\",\""))
		}
	} else {
		fatal(1, "no items returned")
	}
}

func main() {
	switch flag.Parse(); flag.Arg(0) {
	case "series":
		args.seriesSet.Parse(flag.Args()[1:])
		command, args.more = "Series", args.seriesSet.Args()
	case "stream":
		args.streamSet.Parse(flag.Args()[1:])
		command, args.more = "StreamCUR", args.streamSet.Args()
	case "":
		args.more = flag.Args()
	default:
		fatal(1, "unknown subcommand")
	}

	args.settings = cmon.Getarg([]string{args.settings, "CMON_SETTINGS", ".cmon_settings.json"})
	if err := settings.Load(args.settings); err != nil {
		fatal(1, "%v", err)
	}
	address = cmon.Getarg([]string{args.address, settings.Address, "CMON_ADDRESS", ":4404"})

	map[string]func(){
		"Series":    seriesCmd,
		"StreamCUR": streamcurCmd,
		"":          defaultCmd,
	}[command]()
}
