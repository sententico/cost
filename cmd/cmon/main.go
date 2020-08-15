package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type (
	// AWSService settings
	AWSService struct {
		Options  string
		Accounts map[string]map[string]int
	}
	// DatadogService settings
	DatadogService struct {
		APIKey, AppKey string
	}

	// MonSettings are composite settings for the cloud monitor
	MonSettings struct {
		Unit, Port      string
		WorkDir, BinDir string
		Models          map[string]string
		AWS             AWSService
		Datadog         DatadogService
	}

	modSt  uint8
	accTyp uint8
	modRq  struct {
		typ accTyp
		acc chan uint32
	}
	model struct {
		stat       modSt
		req        chan modRq
		rel        chan uint32
		boot, term func(string, chan string)
		maint      func(string)
		data       interface{}
	}
)

const (
	// model states
	msNIL modSt = iota
	msINIT
	msTERM
)
const (
	// model access types
	atEXCL accTyp = 1 << iota
	atLONG
	atPRI
)

var (
	// cloud monitor globals
	sig                    chan os.Signal    // termination signal channel
	seID, seB, seE         chan int64        // session counters
	sfile, port            string            // ...
	srv                    *http.Server      // HTTP server
	mMod                   map[string]*model // monitored object models
	logD, logI, logW, logE *log.Logger       // ...
	seOpen, exit           int               // ...
	seInit, seSeq          int64             // ...
	settings               MonSettings       // ...
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	log.SetFlags(0)
	logD = log.New(os.Stderr, "DEBUG ", log.Lshortfile|log.Lmicroseconds)
	logI = log.New(os.Stderr, "", 0)
	logW = log.New(os.Stderr, "WARNING ", log.Lshortfile)
	logE = log.New(os.Stderr, "ERROR ", log.Lshortfile)

	m, val := map[string]*model{
		"ec2.aws": {boot: ec2awsBoot, maint: ec2awsMaint, term: ec2awsTerm},
		"rds.aws": {boot: rdsawsBoot, maint: rdsawsMaint, term: rdsawsTerm},
	}, func(pri string, dflt string) string {
		if strings.HasPrefix(pri, "CMON_") {
			pri = os.Getenv(pri)
		}
		if pri == "" {
			return dflt
		}
		return pri
	}
	flag.StringVar(&sfile, "settings", val("CMON_SETTINGS", ".cmon_settings.json"), "main settings file")
	flag.Parse()
	if b, err := ioutil.ReadFile(sfile); err != nil {
		logE.Fatalf("cannot read settings file %q (%v)", sfile, err)
	} else if err = json.Unmarshal(b, &settings); err != nil {
		logE.Fatalf("%q is invalid JSON settings file (%v)", sfile, err)
	}
	for n := range m {
		if _, ok := settings.Models[n]; !ok {
			delete(m, n)
		}
	}
	if mMod, port = m, val(strings.TrimLeft(settings.Port, ":"), "4404"); len(m) == 0 {
		logE.Fatalf("no supported objects to monitor specified in %q", sfile)
	}

	seID, seB, seE = make(chan int64, 16), make(chan int64, 16), make(chan int64, 16)
	seInit = time.Now().UnixNano()
	seSeq = seInit
	mux := http.NewServeMux()
	mux.Handle("/admin", apiSession(admin))
	mux.Handle("/api/v0", apiSession(api0))
	mux.Handle("/api/v0/vms", apiSession(api0VMs))
	mux.Handle("/api/v0/disks", apiSession(api0Disks))
	mux.Handle("/api/v0/dbs", apiSession(api0DBs))
	srv = &http.Server{
		Addr:           ":" + port,
		Handler:        mux,
		ReadTimeout:    12 * time.Second,
		WriteTimeout:   12 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
}

func modManager(m *model, n string, ctl chan string) {
	var mr modRq
	var accessors, token uint32
	m.req = make(chan modRq, 16)
	m.rel = make(chan uint32, 16)
	m.boot(n, ctl)

	for ; ; token++ { // loop indefinitely as model access manager when boot complete
	nextRequest:
		for mr = <-m.req; mr.typ&atEXCL == 0; token++ {
			mr.acc <- token
			for accessors++; ; accessors-- {
				select {
				case mr = <-m.req:
					continue nextRequest
				case <-m.rel:
				}
			}
		}
		for ; accessors > 0; accessors-- {
			<-m.rel
		}
		mr.acc <- token
		<-m.rel
	}
}

func seManager(quit <-chan bool, ok chan<- bool) {
	var lc int64
	var to <-chan time.Time

	for id, t := seID, time.NewTicker(60*time.Second); seID != nil || seOpen > 0; {
	nextSelect:
		select {
		case <-t.C:
			if nc := seSeq - seInit - int64(len(seID)); nc > lc {
				logI.Printf("handled %v sessions", nc-lc)
				lc = nc
			}
		case <-to:
			break nextSelect
		case <-seB:
			seOpen++
		case <-seE:
			seOpen--
		case seID <- seSeq:
			seSeq++
		case <-quit:
			seID, to = nil, time.After(3000*time.Millisecond)
			seSeq -= int64(len(id))
			t.Stop()
		}
	}
	ok <- true
}

func apiSession(f func() func(int64, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	session := f()
	return func(w http.ResponseWriter, r *http.Request) {
		id := <-seID
		seB <- id
		session(id, w, r)
		seE <- id
	}
}

func main() {
	logI.Printf("booting %v monitored object models", len(mMod))
	ctl := make(chan string, 4)
	for n, m := range mMod {
		go modManager(m, n, ctl)
	}
	for i := 0; i < len(mMod); i++ {
		n := <-ctl
		o := mMod[n]
		o.stat = msINIT
		logI.Printf("%q object model booted", n)
		go o.maint(n)
	}

	logI.Printf("listening on port %v for HTTP requests", port)
	go func() {
		quit, ok := make(chan bool), make(chan bool)
		go seManager(quit, ok)
		switch s := <-sig; s {
		case syscall.SIGINT, syscall.SIGTERM:
			logI.Printf("beginning signaled shutdown")
		case nil:
		default:
			logE.Printf("beginning shutdown on unexpected %v signal", s)
			exit = 1
		}
		quit <- true
		<-ok
		srv.Close()
		for n, o := range mMod {
			go o.term(n, ctl)
		}
	}()
	switch err := srv.ListenAndServe(); err {
	case nil, http.ErrServerClosed:
		logI.Printf("stopped listening for HTTP requests (%v sessions open)", seOpen)
	default:
		logE.Printf("beginning shutdown on HTTP listener failure (%v)", err)
		exit = 1
	}
	sig <- nil

	for i := 0; i < len(mMod); i++ {
		n := <-ctl
		mMod[n].stat = msTERM
		logI.Printf("%q object model shutdown", n)
	}
	logI.Printf("shutdown complete with %v sessions handled", seSeq-seInit-int64(len(seID)))
	os.Exit(exit)
}
