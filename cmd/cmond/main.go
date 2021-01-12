package main

import (
	"encoding/gob"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sententico/cost/cmon"
)

type (
	modSt  uint8
	accTok uint32
	model  struct {
		name              string
		state             modSt
		immed             bool
		reqP, reqR, reqW  chan chan accTok
		rel               chan accTok
		evt               chan string
		boot, term, maint func(*model)
		persist           int
		data              []interface{}
	}
	modAcc struct {
		m   *model
		tc  chan accTok
		tok accTok
	}
)

const (
	// model states
	msNIL modSt = iota
	msINIT
	msTERM
)

const (
	// access token masks
	atNIL accTok = iota
	atRD
	atWR
	atTYP
	atSEQ
)

const (
	tokExpire = 30 // model access token expiration (seconds)
)

var (
	// cloud monitor globals
	sig                    chan os.Signal    // termination signal channel
	ctl                    chan string       // model control channel
	evt                    chan string       // event broadcast channel
	seID, seB, seE         chan int64        // session counters
	sfile, port            string            // ...
	srv                    *http.Server      // HTTP/REST server
	gosrv, go0srv          *rpc.Server       // GoRPC admin/v0 servers
	mMod                   map[string]*model // monitored object models
	logD, logI, logW, logE *log.Logger       // ...
	seOpen, exit           int               // ...
	seInit, seSeq          int64             // ...
	settings               cmon.MonSettings  // monitor settings
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	log.SetFlags(0)
	logD = log.New(os.Stderr, "DEBUG ", log.Lshortfile|log.Lmicroseconds)
	logI = log.New(os.Stderr, "", 0)
	logW = log.New(os.Stderr, "WARNING ", 0)
	logE = log.New(os.Stderr, "ERROR ", log.Lshortfile)

	mMod, sfile = map[string]*model{
		"trig.cmon": {boot: trigcmonBoot, maint: trigcmonMaint, term: trigcmonTerm, immed: true},
		"ec2.aws":   {boot: ec2awsBoot, maint: ec2awsMaint, term: ec2awsTerm},
		"ebs.aws":   {boot: ebsawsBoot, maint: ebsawsMaint, term: ebsawsTerm},
		"rds.aws":   {boot: rdsawsBoot, maint: rdsawsMaint, term: rdsawsTerm},
		"cur.aws":   {boot: curawsBoot, maint: curawsMaint, term: curawsTerm},
		"cdr.asp":   {boot: cdraspBoot, maint: cdraspMaint, term: cdraspTerm},
	}, cmon.Getarg([]string{"CMON_SETTINGS", ".cmon_settings.json"})
	if err := settings.Load(sfile); err != nil {
		logE.Fatalf("%v", err)
	}
	for n, m := range mMod {
		if _, found := settings.Models[n]; found {
			m.name = n
		} else if delete(mMod, n); len(mMod) == 0 {
			logE.Fatalf("no supported objects to monitor specified in %q", sfile)
		}
	}
	port = cmon.Getarg([]string{settings.Address, "CMON_ADDRESS", ":4404"})
	if addr := strings.Split(port, ":"); len(addr) > 1 {
		port = addr[len(addr)-1]
	}

	ctl, evt = make(chan string, 4), make(chan string, 4)
	seID, seB, seE = make(chan int64, 16), make(chan int64, 16), make(chan int64, 16)
	seInit = time.Now().UnixNano()
	seSeq = seInit
	http.HandleFunc("/admin", sessHandler(admin))
	http.HandleFunc("/gorpc/v0", sessHandler(gorpc0))
	http.HandleFunc("/rest/v0", sessHandler(rest0))
	http.HandleFunc("/rest/v0/vms", sessHandler(rest0VMs))
	http.HandleFunc("/rest/v0/disks", sessHandler(rest0Disks))
	http.HandleFunc("/rest/v0/dbs", sessHandler(rest0DBs))
	srv, gosrv, go0srv = &http.Server{
		Addr:           ":" + port,
		ReadTimeout:    12 * time.Second,
		WriteTimeout:   12 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}, rpc.NewServer(), rpc.NewServer()
	gosrv.Register(&Admin{})
	go0srv.Register(&API{Ver: 0})

	gob.Register(&trigModel{})
	gob.Register(&ec2Sum{})
	gob.Register(&ec2Detail{})
	gob.Register(&ebsSum{})
	gob.Register(&ebsDetail{})
	gob.Register(&rdsSum{})
	gob.Register(&rdsDetail{})
	gob.Register(&curSum{})
	gob.Register(&curDetail{})
	gob.Register(&termSum{})
	gob.Register(&origSum{})
	gob.Register(&termDetail{})
	gob.Register(&origDetail{})
}

func modManager(m *model) {
	m.reqP, m.reqR, m.reqW = make(chan chan accTok, 16), make(chan chan accTok, 16), make(chan chan accTok, 16)
	m.rel = make(chan accTok, 16)
	m.evt = make(chan string, 4)
	m.boot(m)
	ctl <- m.name // signal boot complete; enter model access manager loop

	var tc chan accTok
	var tok, write, tokseq accTok
	reqw, read, ttl := m.reqW, make(map[accTok]int16), int16(0)
	for tick, tock := time.NewTicker(1*time.Second), func() {
		for tok, ttl = range read {
			if ttl <= 1 {
				delete(read, tok)
				logE.Printf("%q read access token expired", m.name)
				continue
			}
			read[tok]--
		}
	}; ; {
		select {
		case <-tick.C:
			tock()
		case tc = <-m.reqR:
			tok = tokseq | atRD
			tc <- tok
			tokseq += atSEQ
			read[tok], reqw = tokExpire, nil
			continue
		case tok = <-m.rel:
			if delete(read, tok); len(read) == 0 {
				reqw = m.reqW
			}
			continue
		case tc = <-m.reqP:
			for reqw = m.reqW; len(read) > 0; {
				select {
				case <-tick.C:
					tock()
				case tok = <-m.rel:
					delete(read, tok)
				}
			}
		case tc = <-reqw:
		}
		write = tokseq | atWR
		tc <- write
		tokseq += atSEQ
		for ttl = tokExpire; ; {
			select {
			case <-tick.C:
				if ttl--; ttl > 0 {
					continue
				}
				logE.Printf("%q write access token expired", m.name)
			case tok = <-m.rel:
				if tok != write {
					continue
				}
			}
			break
		}
	}
}

func (m *model) newAcc() *modAcc {
	if m == nil {
		return nil
	}
	return &modAcc{
		m:  m,
		tc: make(chan accTok, 1),
	}
}
func (acc *modAcc) reqP() {
	if acc != nil {
		switch acc.tok & atTYP {
		case atWR:
		case atRD:
			acc.rel()
			fallthrough
		case atNIL:
			acc.m.reqP <- acc.tc
			acc.tok = <-acc.tc
		}
	}
}
func (acc *modAcc) reqR() {
	if acc != nil {
		switch acc.tok & atTYP {
		case atRD:
		case atWR:
			acc.rel()
			fallthrough
		case atNIL:
			acc.m.reqR <- acc.tc
			acc.tok = <-acc.tc
		}
	}
}
func (acc *modAcc) reqW() {
	if acc != nil {
		switch acc.tok & atTYP {
		case atWR:
		case atRD:
			acc.rel()
			fallthrough
		case atNIL:
			acc.m.reqW <- acc.tc
			acc.tok = <-acc.tc
		}
	}
}
func (acc *modAcc) rel() bool {
	if acc != nil && acc.tok&atTYP != atNIL {
		acc.m.rel <- acc.tok
		acc.tok &^= atTYP
		return true
	}
	return false
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

// TODO: add session methods?

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

func sessHandler(f func() func(int64, http.ResponseWriter, *http.Request)) http.HandlerFunc {
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
	for _, m := range mMod {
		go modManager(m)
	}
	for range mMod {
		logI.Printf("%q object model booted", <-ctl)
	}
	dseq := 0
	for _, m := range mMod {
		if m.state = msINIT; m.immed {
			go m.maint(m)
		} else {
			d, m := time.Duration(dseq*100)*time.Second, m
			dseq++
			goAfter(d, d+20*time.Second, func() { m.maint(m) })
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
		for _, m := range mMod {
			m := m
			go func() { m.term(m); ctl <- m.name }()
		}
	}()
	switch err := srv.ListenAndServe(); err {
	case nil, http.ErrServerClosed:
		logI.Printf("stopped listening for HTTP port %v requests (%v sessions open)", port, seOpen)
	default:
		logE.Printf("beginning shutdown on HTTP port %v listener failure: %v", port, err)
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
