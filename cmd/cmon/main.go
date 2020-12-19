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
	interval [2]int32
)

var (
	args struct {
		settings  string   // settings location
		address   string   // settings address:port
		debug     bool     // enables debug output
		more      []string // unparsed arguments
		metric    string   // series metric
		items     int      // maximum item stream count
		threshold float64  // series/stream filter threshold
		hours     interval // hour interval
		history   time.Duration
		recent    time.Duration
		seriesSet *flag.FlagSet
		streamSet *flag.FlagSet
	}
	address  string           // cmon address (args override settings file)
	settings cmon.MonSettings // settings
	command  string           // requested command
	wg       sync.WaitGroup   // thread synchronization
)

func init() {
	flag.StringVar(&args.settings, "s", "", "settings `file`")
	flag.StringVar(&args.address, "a", "", "cmon server location `address:port`")
	flag.BoolVar(&args.debug, "d", false, fmt.Sprintf("specify debug output"))

	args.seriesSet = flag.NewFlagSet("series", flag.ExitOnError)
	args.seriesSet.StringVar(&args.metric, "metric", "cdr.asp/term/geo/n", "series metric `name`")
	args.seriesSet.DurationVar(&args.history, "history", time.Hour*12, "hourly series total `duration`")
	args.seriesSet.DurationVar(&args.recent, "recent", time.Hour*3, "`duration` of recent/active part of series")
	args.seriesSet.Float64Var(&args.threshold, "threshold", 0, "series filter threshold `amount`")

	args.streamSet = flag.NewFlagSet("stream", flag.ExitOnError)
	args.streamSet.Var(&args.hours, "hours", "`YYYY-MM[-DD[Thh]][,[...]]` interval to stream")
	args.streamSet.IntVar(&args.items, "items", 1000, "`maximum` items to stream")
	args.streamSet.Float64Var(&args.threshold, "threshold", 0.01, "stream filter threshold `amount`")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "\ncommand usage: cmon [-s] [-a] [-d] <subcommand> [<subcommand arg> ...]"+
			"\n\nThis is the command-line interface to the Cloud Monitor. Subcommands generally map to API interfaces and return model content within the Cloud Monitor\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nThe \"series\" subcommand returns a metric hourly series.\n")
		args.seriesSet.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nThe \"stream\" subcommand returns an item detail stream.\n")
		args.streamSet.PrintDefaults()
		fmt.Fprintln(flag.CommandLine.Output())
	}
}

func (i *interval) String() string {
	return fmt.Sprintf("[%v,%v]", i[0], i[1])
}
func (i *interval) Set(arg string) error {
	switch s, t, err := strings.Split(arg, ","), time.Now(), error(nil); len(s) {
	case 1:
		switch len(s[0]) {
		case 7:
			if t, err = time.Parse(time.RFC3339, s[0]+"-01T00:00:00Z"); err == nil {
				i[0], i[1] = int32(t.Unix())/3600, int32(t.AddDate(0, 1, 0).Unix()-1)/3600
				return nil
			}
		case 10:
			if t, err := time.Parse(time.RFC3339, s[0]+"T00:00:00Z"); err == nil {
				i[0], i[1] = int32(t.Unix())/3600, int32(t.AddDate(0, 0, 1).Unix()-1)/3600
				return nil
			}
		case 13:
			if t, err := time.Parse(time.RFC3339, s[0]+":00:00Z"); err == nil {
				i[0] = int32(t.Unix()) / 3600
				i[1] = i[0]
				return nil
			}
		}
		return fmt.Errorf("%v", err)
	default:
		return fmt.Errorf("invalid argument")
	}
}

func fatal(ex int, format string, a ...interface{}) {
	fmt.Printf(format, a...)
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
			fatal(1, "error calling GoRPC: %v\n", err)
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
		History:   int((args.history + time.Hour - time.Nanosecond) / time.Hour),
		Recent:    int((args.recent + time.Hour - time.Nanosecond) / time.Hour),
		Threshold: args.threshold,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v\n", err)
	}
	client.Close()
	for k, ser := range r {
		fmt.Printf("%v: %v\n", k, ser)
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
		From:      args.hours[0],
		To:        args.hours[1],
		Items:     args.items,
		Threshold: args.threshold,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v\n", err)
	}
	client.Close()
	for _, row := range r {
		fmt.Printf("\"%v\"\n", strings.Join(row, "\",\""))
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
	default:
		args.more = flag.Args()
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
