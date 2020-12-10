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

	"github.com/sententico/cost/cmon"
)

var (
	settingsFlag string
	debugFlag    bool
	port         string
	settings     cmon.MonSettings
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

func main() {
	client, err := rpc.DialHTTPPath("tcp", ":"+port, "/gorpc/v0")
	for in, r := bufio.NewScanner(os.Stdin), ""; err == nil && in.Scan(); err = client.Call("Test0.Upper", &cmon.Test0{S: in.Text()}, &r) {
		fmt.Printf("%v\n", r)
	}
	if err != nil {
		fatal(1, "GoRPC error: %v\n", err)
	}
}
