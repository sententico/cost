package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type (
	// awsService settings
	awsService struct {
		Options  string
		Accounts map[string]map[string]float32
	}
	// datadogService settings
	datadogService struct {
		APIKey, AppKey string
	}

	// monSettings are composite settings for the cloud monitor
	monSettings struct {
		Unit, Port      string
		WorkDir, BinDir string
		Models          map[string]string
		AWS             awsService
		Datadog         datadogService
	}

	modSt  uint8
	accTok uint32
	accTyp uint8
	modRq  struct {
		typ accTyp
		acc chan accTok
	}
	model struct {
		state      modSt
		immed      bool
		req        chan modRq
		rel        chan accTok
		evt        chan string
		boot, term func(string, chan string)
		maint      func(string)
		persist    int
		data       []interface{}
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
	evt                    chan string       // event broadcast channel
	seID, seB, seE         chan int64        // session counters
	sfile, port            string            // ...
	srv                    *http.Server      // HTTP server
	mMod                   map[string]*model // monitored object models
	logD, logI, logW, logE *log.Logger       // ...
	seOpen, exit           int               // ...
	seInit, seSeq          int64             // ...
	settings               monSettings       // ...
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
		"trig.cmon": {boot: trigcmonBoot, maint: trigcmonMaint, term: trigcmonTerm, immed: true},
		"ec2.aws":   {boot: ec2awsBoot, maint: ec2awsMaint, term: ec2awsTerm},
		"ebs.aws":   {boot: ebsawsBoot, maint: ebsawsMaint, term: ebsawsTerm},
		"rds.aws":   {boot: rdsawsBoot, maint: rdsawsMaint, term: rdsawsTerm},
		"cdr.asp":   {boot: cdraspBoot, maint: cdraspMaint, term: cdraspTerm},
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
		logE.Fatalf("cannot read settings file %q: %v", sfile, err)
	} else if err = json.Unmarshal(b, &settings); err != nil {
		logE.Fatalf("%q is invalid JSON settings file: %v", sfile, err)
	}
	for n := range m {
		if _, found := settings.Models[n]; !found {
			delete(m, n)
		}
	}
	if mMod, port = m, val(strings.TrimLeft(settings.Port, ":"), "4404"); len(m) == 0 {
		logE.Fatalf("no supported objects to monitor specified in %q", sfile)
	}

	evt = make(chan string, 4)
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
	var accessors, token accTok
	m.req = make(chan modRq, 16)
	m.rel = make(chan accTok, 16)
	m.evt = make(chan string, 4)
	m.boot(n, ctl)

	for token++; ; token++ { // loop indefinitely as model access manager when boot complete
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
	var to <-chan time.Time
	min, id, lc := time.NewTicker(60*time.Second), seID, int64(0)

	for {
		select {
		case seID <- seSeq: // serve session IDs until quit signaled
			seSeq++
		case <-seB:
			seOpen++
		case <-seE:
			if seOpen--; seID == nil && to != nil && seOpen == 0 {
				ok <- true
				to = nil
			}

		case e := <-evt:
			for n, m := range mMod {
				if n != e { // broadcast model events to all other models
					select {
					case m.evt <- e:
					default:
					}
				}
			}

		case <-min.C:
			if nc := seSeq - seInit - int64(len(seID)); nc > lc {
				if sh := nc - lc; sh > 12 {
					logI.Printf("handled %v sessions", sh)
				}
				lc = nc
			}
		case <-to: // after quit requested, ack once no open sessions or timeout
			ok <- true
			to = nil

		case <-quit:
			if seID, quit = nil, nil; seOpen > 0 {
				to = time.After(3000 * time.Millisecond)
			} else {
				ok <- true
			}
			min.Stop()
			seSeq -= int64(len(id))
		}
	}
}

func goAfter(low time.Duration, high time.Duration, f func()) {
	if low == 0 && high == 0 || low < 0 {
		go f()
	} else if low >= high {
		time.AfterFunc(low, f)
	} else {
		time.AfterFunc(low+time.Duration(rand.Int63n(int64(high-low))), f)
	}
}

func goaftSession(low time.Duration, high time.Duration, f func()) {
	if g := func() {
		id := <-seID
		seB <- id
		f()
		seE <- id
	}; low == 0 && high == 0 || low < 0 {
		go g()
	} else if low >= high {
		time.AfterFunc(low, g)
	} else {
		time.AfterFunc(low+time.Duration(rand.Int63n(int64(high-low))), g)
	}
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
	ctl, dseq := make(chan string, 4), 0
	for n, m := range mMod {
		go modManager(m, n, ctl)
	}
	for range mMod {
		n := <-ctl
		logI.Printf("%q object model booted", n)
	}
	for n, m := range mMod {
		if m.state = msINIT; m.immed {
			go m.maint(n)
		} else {
			d, m, n := time.Duration(dseq*100)*time.Second, m, n
			dseq++
			goAfter(d, d+20*time.Second, func() { m.maint(n) })
		}
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
		for n, m := range mMod {
			go m.term(n, ctl)
		}
	}()
	switch err := srv.ListenAndServe(); err {
	case nil, http.ErrServerClosed:
		logI.Printf("stopped listening for HTTP requests (%v sessions open)", seOpen)
	default:
		logE.Printf("beginning shutdown on HTTP listener failure: %v", err)
		exit = 1
	}
	sig <- nil

	for range mMod {
		n := <-ctl
		mMod[n].state = msTERM
		logI.Printf("%q object model shutdown", n)
	}
	logI.Printf("shutdown complete with %v sessions handled", seSeq-seInit-int64(len(seID)))
	os.Exit(exit)
}
