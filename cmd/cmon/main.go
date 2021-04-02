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
		address  string   // cmon server address:port
		debug    bool     // debug output enabled
		settings string   // settings location
		more     []string // unparsed arguments

		seriesSet *flag.FlagSet
		metric    string  // series metric
		recent    int     // series recent/active hours
		span      int     // series total hours
		seTrunc   float64 // series recent sum truncation filter

		tableSet *flag.FlagSet
		interval intHours // from/to/units hours
		model    string   // table object model
		rows     int      // maximum table rows
		taTrunc  float64  // row cost truncation filter
	}
	address  string           // cmon server address (args override settings file)
	settings cmon.MonSettings // settings
	command  string           // cmon subcommand
	wg       sync.WaitGroup   // thread synchronization
)

func init() {
	flag.StringVar(&args.address, "a", "", "cmond server location `address:port`")
	flag.BoolVar(&args.debug, "d", false, fmt.Sprintf("specify debug output"))
	flag.StringVar(&args.settings, "s", "", "settings `file`")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThis is the client command-line interface to Cloud Monitor (cmond) servers. Cloud Monitors gather cost,"+
				"\nusage and other data from configured cloud objects, using this data to maintain models representing these"+
				"\nobjects.  Subcommands map to server API interfaces exposing this model content."+
				"\n  Usage: cmon [-a] [-d] [-s] <subcommand> [<subcommand arg> ...]\n\n")
		flag.PrintDefaults()
		args.seriesSet.Usage()
		args.tableSet.Usage()
		fmt.Fprintln(flag.CommandLine.Output())
	}

	args.seriesSet = flag.NewFlagSet("series", flag.ExitOnError)
	args.seriesSet.StringVar(&args.metric, "metric", "cdr.asp/term/geo/n", "series metric `type`")
	args.seriesSet.IntVar(&args.recent, "recent", 3, "`hours` of recent/active part of span")
	args.seriesSet.IntVar(&args.span, "span", 12, "series total `hours`")
	args.seriesSet.Float64Var(&args.seTrunc, "truncate", 0, "metric filter threshold applied to the `average` of recent hours in the overall series span")
	args.seriesSet.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"series\" subcommand returns an hourly series for each metric of a -metric type. Metrics are filtered"+
				"\nby the -truncate threshold applied to the average of -recent hours in the overall series -span."+
				"\n  Usage: cmon series [<series arg> ...]\n\n")
		args.seriesSet.PrintDefaults()
	}

	args.tableSet = flag.NewFlagSet("table", flag.ExitOnError)
	y, m, _ := time.Now().AddDate(0, 0, -1).Date() // set default to prior month, compensating for CUR lag
	t := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	args.interval = intHours{int32(t.AddDate(0, -1, 0).Unix() / 3600), int32((t.Unix() - 1) / 3600), 720}
	args.tableSet.Var(&args.interval, "interval", "`YYYY-MM[-DD[Thh]][+r]` month/day/hour +range to return, if applicable")
	args.tableSet.StringVar(&args.model, "model", "cur.aws", "table object model `name`")
	args.tableSet.IntVar(&args.rows, "rows", 2e5, "`maximum` table rows to return")
	args.tableSet.Float64Var(&args.taTrunc, "truncate", 0.002, "row `cost` filter threshold, if applicable")
	args.tableSet.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nThe \"table\" subcommand returns filtered -model detail in table/CSV form. Column criteria are specified"+
				"\nas column/operator/operand tuples (e.g.: 'Acct]dev' 'Type~\\.[24]?xl' 'Tries>1' 'Since<2021-02-13T15')"+
				"\n  Usage: cmon table [<table arg> ...] ['<column criterion>' ...]\n\n")
		args.tableSet.PrintDefaults()
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
	var r cmon.SeriesRet
	if err = client.Call("API.Series", &cmon.SeriesArgs{
		Token:    "placeholder_access_token",
		Metric:   args.metric,
		Span:     args.span,
		Recent:   args.recent,
		Truncate: args.seTrunc,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	}
	client.Close()
	for k, ser := range r.Series {
		if f, alt := fmt.Sprintf("%.2f", ser), fmt.Sprintf("%.6g", ser); len(alt) < len(f) {
			fmt.Printf("%v: %s\n", k, alt)
		} else {
			fmt.Printf("%v: %s\n", k, f)
		}
	}
}

func tableCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r [][]string
	if err = client.Call("API.Table", &cmon.TableArgs{
		Token:    "placeholder_access_token",
		Model:    args.model,
		Rows:     args.rows,
		Criteria: args.more,
	}, &r); err != nil {
		fatal(1, "error calling GoRPC: %v", err)
	}
	if client.Close(); len(r) > 0 {
		switch args.model {
		case "ec2.aws":
			fmt.Println("Inst,Acct,Type,Plat,Vol,AZ,AMI,Spot," +
				"Name,env,dc,product,app,cust,team,version,State,Since,Active%,ORate,Rate")
		case "ebs.aws":
			fmt.Println("Vol,Acct,Type,Size,IOPS,AZ,Mount," +
				"Name,env,dc,product,app,cust,team,version,State,Since,Active%,Rate")
		case "rds.aws":
			fmt.Println("DB,Acct,Type,Sto,Size,IOPS,Engine,EngVer,Lic,AZ," +
				"Name,env,dc,product,app,cust,team,version,State,Since,Active%,Rate")
		case "cdr.asp/term", "cdr.asp/orig":
			fmt.Println("CDR,Loc,To,From,Prov,Cust/App,Start,Min,Tries,Billable,Margin")
		}
		for _, row := range r {
			fmt.Printf("\"%s\"\n", strings.Join(row, "\",\"")) // assumes no double-quotes in fields
		}
	} else {
		fatal(1, "no rows returned")
	}
}

func curtabCmd() {
	client, err := rpc.DialHTTPPath("tcp", address, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}
	var r [][]string
	if err = client.Call("API.CURtab", &cmon.CURtabArgs{
		Token:    "placeholder_access_token",
		From:     args.interval.from,
		To:       args.interval.to,
		Units:    args.interval.units,
		Rows:     args.rows,
		Truncate: args.taTrunc,
		Criteria: args.more,
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
		if len(r) == args.rows {
			warn = " [row max]"
		}
		fmt.Printf("Invoice Item%s,%s,AWS Account,Type,Service,Usage Type,Operation,Region,Resource ID"+
			",Item Description,Name,env,dc,product,app,cust,team,version,Recs,Usage,Billed\n", warn, unit)
		for _, row := range r {
			fmt.Printf("\"%s\"\n", strings.Join(row, "\",\"")) // assumes no double-quotes in fields
		}
	} else {
		fatal(1, "no items returned")
	}
}

func main() {
	switch flag.Parse(); flag.Arg(0) {
	case "series":
		args.seriesSet.Parse(flag.Args()[1:])
		command, args.more = "series", args.seriesSet.Args()
	case "table":
		args.tableSet.Parse(flag.Args()[1:])
		command, args.more = "table "+args.model, args.tableSet.Args()
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

	if cfn := map[string]func(){
		"series":             seriesCmd,
		"table cur.aws":      curtabCmd,
		"table ec2.aws":      tableCmd,
		"table ebs.aws":      tableCmd,
		"table rds.aws":      tableCmd,
		"table cdr.asp/term": tableCmd,
		"table cdr.asp/orig": tableCmd,
		"":                   defaultCmd,
	}[command]; cfn == nil {
		fatal(1, "can't get %s", command)
	} else {
		cfn()
	}
}
