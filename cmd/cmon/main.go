package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/sententico/cost/cmon"
)

var (
	settingsFlag string
	debugFlag    bool
	port         string
	settings     cmon.MonSettings
	wg           sync.WaitGroup
)

func init() {
	val := func(pri string, dflt string) string {
		if strings.HasPrefix(pri, "CMON_") {
			pri = os.Getenv(pri)
		}
		if pri == "" {
			return dflt
		}
		return pri
	}

	flag.StringVar(&settingsFlag, "settings", val("CMON_SETTINGS", ".cmon_settings.json"), "settings file")
	flag.BoolVar(&debugFlag, "d", false, fmt.Sprintf("specify debug output"))
	flag.Usage = func() {
		fmt.Printf("command usage: cmon ..." +
			"\n\nThis command ...\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if b, err := ioutil.ReadFile(settingsFlag); err != nil {
		fatal(1, "cannot read settings file %q: %v\n", settingsFlag, err)
	} else if err = json.Unmarshal(b, &settings); err != nil {
		fatal(1, "%q is invalid JSON settings file: %v\n", settingsFlag, err)
	}
	port = val(strings.TrimLeft(settings.Port, ":"), "4404")
}

func fatal(ex int, format string, a ...interface{}) {
	fmt.Printf(format, a...)
	os.Exit(ex)
}

func worker(in chan string) {
	client, err := rpc.DialHTTPPath("tcp", ":"+port, "/gorpc/v0")
	if err != nil {
		fatal(1, "error dialing GoRPC server: %v", err)
	}

	var r string
	for ln := range in {
		if err = client.Call("Test0.Upper", &cmon.Test0{S: ln}, &r); err != nil {
			fatal(1, "error calling GoRPC: %v\n", err)
		}
		fmt.Printf("%v\n", r)
	}
	client.Close()
	wg.Done()
}

func main() {
	in := make(chan string, 20)
	go func(ln *bufio.Scanner, in chan string) {
		for ; ln.Scan(); in <- ln.Text() {
		}
		close(in)
	}(bufio.NewScanner(os.Stdin), in)

	for i := 0; i < cap(in)/2; i++ {
		wg.Add(1)
		go worker(in)
	}
	wg.Wait()
}
