package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sententico/cost/cmon"
)

type (
	interval struct {
		from, to int32
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
		threshold float64  // series/stream filter return threshold
		hours     interval // from/to hours interval
		history   int      // series total hours
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

	args.seriesSet = flag.NewFlagSet("series", flag.ExitOnError)
	args.seriesSet.StringVar(&args.metric, "metric", "cdr.asp/term/geo/n", "series metric `name`")
	args.seriesSet.IntVar(&args.history, "history", 12, "series total duration in `hours`")
	args.seriesSet.IntVar(&args.recent, "recent", 3, "`hours` of recent/active part of series")
	args.seriesSet.Float64Var(&args.threshold, "threshold", 0, "series filter threshold `amount`")

	args.streamSet = flag.NewFlagSet("stream", flag.ExitOnError)
	y, m, _ := time.Now().Date()
	t := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	args.hours = interval{int32(t.AddDate(0, -1, 0).Unix() / 3600), int32((t.Unix() - 1) / 3600)}
	args.streamSet.Var(&args.hours, "hours", "YYYY-MM[-DD[Thh]][,[...]] `interval` to stream")
	args.streamSet.IntVar(&args.items, "items", 1000, "`maximum` items to stream")
	args.streamSet.Float64Var(&args.threshold, "threshold", 0.005, "stream filter threshold `amount`")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nCommand usage: cmon [-s] [-a] [-d] <subcommand> [<subcommand arg> ...]"+
				"\n\nThis is the command-line interface to the Cloud Monitor. Subcommands generally map to API"+
				"\ninterfaces and return model content within the Cloud Monitor.\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"series\" subcommand returns a metric hourly series.\n")
		args.seriesSet.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"stream\" subcommand returns an item detail stream.\n")
		args.streamSet.PrintDefaults()
		fmt.Fprintln(flag.CommandLine.Output())
	}
}

func (h *interval) String() string {
	if h != nil {
		return fmt.Sprintf("%v,%v",
			time.Unix(int64(h.from)*3600, 0).UTC().Format(time.RFC3339)[:13],
			time.Unix(int64(h.to)*3600, 0).UTC().Format(time.RFC3339)[:13])
	}
	return ""
}
func (h *interval) Set(arg string) error {
	switch s, t, err := strings.Split(arg, ","), time.Now(), error(nil); len(s) {
	case 1:
		switch len(s[0]) {
		case 7:
			if t, err = time.Parse(time.RFC3339, s[0]+"-01T00:00:00Z"); err == nil {
				h.from, h.to = int32(t.Unix())/3600, int32(t.AddDate(0, 1, 0).Unix()-1)/3600
				return nil
			}
		case 10:
			if t, err := time.Parse(time.RFC3339, s[0]+"T00:00:00Z"); err == nil {
				h.from, h.to = int32(t.Unix())/3600, int32(t.AddDate(0, 0, 1).Unix()-1)/3600
				return nil
			}
		case 13:
			if t, err := time.Parse(time.RFC3339, s[0]+":00:00Z"); err == nil {
				h.from = int32(t.Unix()) / 3600
				h.to = h.from
				return nil
			}
		}
		return fmt.Errorf("%v", err)
	default:
		return fmt.Errorf("invalid argument")
	}
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
	var r cmon.SeriesRet
	if err = client.Call("API.Series", &cmon.SeriesArgs{
		Token:     "placeholder_access_token",
		Metric:    args.metric,
		History:   args.history,
		Recent:    args.recent,
		Threshold: args.threshold,
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
		Token:     "placeholder_access_token",
		From:      args.hours.from,
		To:        args.hours.to,
		Items:     args.items,
		Threshold: args.threshold,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	}
	client.Close()
	if len(r) > 0 {
		fmt.Println("Invoice ID,Date,Account,Type,Service,Usage Type,Usage Operation,Region" +
			",Resource ID,Description,Name,Env,DC,Prod,App,Cust,Team,Ver,Records,Usage,Amount")
		for _, row := range r {
			fmt.Printf("\"%s\"\n", strings.Join(row, "\",\""))
		}
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
