package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type (
	httpRq uint8
	objSt  uint8
	rqTyp  uint8
	objRq  struct {
		rt  rqTyp
		sig chan uint32
	}
	obj struct {
		stat objSt
		req  chan objRq
		rel  chan uint32
		data *interface{}
	}
)

const (
	hrAPI httpRq = iota
	hrTEST
)
const (
	osNIL objSt = iota
	osINIT
	osTERM
)
const (
	rtEXCL rqTyp = 1 << iota
	rtLONG
	rtPRI
)

var (
	sig  chan os.Signal
	srv  *http.Server
	cobj map[string]obj
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	mux, port := http.NewServeMux(), os.Getenv("CMON_PORT")
	mux.Handle("/api", httpMonitor(hrAPI))
	mux.Handle("/test", httpMonitor(hrTEST))
	if port == "" {
		port = "8080"
	}
	srv = &http.Server{
		Addr:           ":" + port,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	cobj = map[string]obj{
		"ec2": {},
		"rds": {},
	}
}

func httpMonitor(hr httpRq) http.HandlerFunc { // pass in args for closure to close over
	return func(w http.ResponseWriter, r *http.Request) {
		switch hr {
		case hrAPI:
			// map to rmonitor
			// select inputs from accessor(s) result(s) & http.CloseNotifier channels
			w.Write([]byte("api"))
		case hrTEST:
			w.Write([]byte("test"))
		}
	}
}

func objManage(o obj, n string, c chan string) {
	var rq objRq
	var token uint32
	o.req = make(chan objRq, 16)
	o.rel = make(chan uint32, 16)
	o.boot(n, c)

	// loop indefinitely as object access manager when boot complete
	for accessors := 0; ; token++ {
		for rq = <-o.req; rq.rt&rtEXCL == 0; token++ {
			rq.sig <- token
			for accessors++; ; {
				if accessors > 0 {
					select {
					case rq = <-o.req:
					case <-o.rel:
						accessors--
						continue
					}
				} else {
					rq = <-o.req
				}
				break
			}
		}
		for ; accessors > 0; accessors-- {
			<-o.rel
		}
		rq.sig <- token
		<-o.rel
	}
}

func main() {
	ctl := make(chan string, 1)
	for n, o := range cobj {
		go objManage(o, n, ctl)
	}
	for i := 0; i < len(cobj); i++ {
		n := <-ctl
		o := cobj[n]
		o.stat = osINIT
		log.Printf("%q object booted", n)
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("cannot listen for HTTP requests: %v", err)
	}
	for n, o := range cobj {
		go o.maint(n)
	}

	log.Printf("signal %v received: beginning shutdown", <-sig)
	srv.Close() // check out srv.Shutdown() alternative?
	for n, o := range cobj {
		go o.term(n, ctl)
	}
	for i := 0; i < len(cobj); i++ {
		n := <-ctl
		o := cobj[n]
		o.stat = osTERM
		log.Printf("%q object shutdown", n)
	}
	log.Printf("graceful shutdown complete")
}
