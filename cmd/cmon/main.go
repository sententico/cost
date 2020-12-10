package main

import (
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

	flag.StringVar(&settingsFlag, "settings", val("CMON_SETTINGS", "~/.cmon_settings.json"), "settings file")
	flag.BoolVar(&debugFlag, "d", false, fmt.Sprintf("specify debug output"))
	flag.Usage = func() {
		fmt.Printf("command usage: cmon ..." +
			"\n\nThis command ...\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if b, err := ioutil.ReadFile(settingsFlag); err != nil {
		fatal(1, "cannot read settings file %q: %v", settingsFlag, err)
	} else if err = json.Unmarshal(b, &settings); err != nil {
		fatal(1, "%q is invalid JSON settings file: %v", settingsFlag, err)
	}
	port = val(strings.TrimLeft(settings.Port, ":"), "4404")
}

func fatal(ex int, format string, a ...interface{}) {
	fmt.Printf(format, a...)
	os.Exit(ex)
}

func main() {
	var r string
	if client, err := rpc.DialHTTPPath("tcp", ":"+port, "/gorpc/v0"); err != nil {
		fatal(1, "error dialing server: %v", err)
	} else if err = client.Call("Test0.Upper",
		&cmon.Test0{S: "Under the spreading chestnut tree"},
		&r); err != nil {
		fatal(1, "RPC error: %v", err)
	}
	fmt.Printf("%v\n", r)
}
