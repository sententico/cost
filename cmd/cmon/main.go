package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/sententico/cost/cmon"
)

var (
	args struct {
		settings  string   // settings location
		address   string   // settings address:port
		debug     bool     // enables debug output
		more      []string // unparsed arguments
		metric    string   // series metric
		items     int      // maximum item stream count
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
	args.seriesSet.StringVar(&args.metric, "metric", "cdr.aws/term/geo", "series `metric`")

	args.streamSet = flag.NewFlagSet("stream", flag.ExitOnError)
	args.streamSet.IntVar(&args.items, "items", 1000, "`maximum` items to stream")

	flag.Usage = func() {
		fmt.Printf("command usage: cmon [-s] [-a] [-d] <subcommand> ..." +
			"\n\nThis command is the command-line interface to the Cloud Monitor.\n\n")
		flag.PrintDefaults()
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
		Metric:    "ec2.aws/sku",
		History:   12,
		Recent:    4,
		Threshold: 0.0,
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
		From:      446600,
		To:        446700,
		Items:     1000,
		Threshold: 0.0,
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
