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
	ObjSettings struct {
		Token string
	}
	MonSettings struct {
		User, Port string
		Objects    map[string]ObjSettings
	}
	objSt  uint8
	accTyp uint8
	objRq  struct {
		typ accTyp
		acc chan uint32
	}
	obj struct {
		stat       objSt
		req        chan objRq
		rel        chan uint32
		boot, term func(string, chan string)
		maint      func(string)
		data       interface{}
	}
)

const ( // object states
	osNIL objSt = iota
	osINIT
	osTERM
)
const ( // object access types
	atEXCL accTyp = 1 << iota
	atLONG
	atPRI
)

var (
	sig                    chan os.Signal
	seID, seB, seE         chan int64
	sfile, port            string
	srv                    *http.Server
	mObj                   map[string]*obj
	logD, logI, logW, logE *log.Logger
	seOpen, exit           int
	seInit, seSeq          int64
	settings               MonSettings
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	log.SetFlags(0)
	logD = log.New(os.Stderr, "DEBUG ", log.Lshortfile|log.Lmicroseconds)
	logI = log.New(os.Stderr, "", 0)
	logW = log.New(os.Stderr, "WARNING ", log.Lshortfile)
	logE = log.New(os.Stderr, "ERROR ", log.Lshortfile)

	o, val := map[string]*obj{
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
	for n := range o {
		if _, ok := settings.Objects[n]; !ok {
			delete(o, n)
		}
	}
	if mObj, port = o, val(strings.TrimLeft(settings.Port, ":"), "4404"); len(o) == 0 {
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

func objManager(o *obj, n string, ctl chan string) {
	var or objRq
	var accessors, token uint32
	o.req = make(chan objRq, 16)
	o.rel = make(chan uint32, 16)
	o.boot(n, ctl)

	for ; ; token++ { // loop indefinitely as object access manager when boot complete
	nextRequest:
		for or = <-o.req; or.typ&atEXCL == 0; token++ {
			or.acc <- token
			for accessors++; ; accessors-- {
				select {
				case or = <-o.req:
					continue nextRequest
				case <-o.rel:
				}
			}
		}
		for ; accessors > 0; accessors-- {
			<-o.rel
		}
		or.acc <- token
		<-o.rel
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
	logI.Printf("booting %v monitored objects", len(mObj))
	ctl := make(chan string, 4)
	for n, o := range mObj {
		go objManager(o, n, ctl)
	}
	for i := 0; i < len(mObj); i++ {
		n := <-ctl
		o := mObj[n]
		o.stat = osINIT
		logI.Printf("%q object booted", n)
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
		for n, o := range mObj {
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

	for i := 0; i < len(mObj); i++ {
		n := <-ctl
		mObj[n].stat = osTERM
		logI.Printf("%q object shutdown", n)
	}
	logI.Printf("shutdown complete with %v sessions handled", seSeq-seInit-int64(len(seID)))
	os.Exit(exit)
}
