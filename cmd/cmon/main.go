package main

import (
	"flag"
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
		acc chan uint32
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
	port string
	srv  *http.Server
	cObj map[string]*obj
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	flag.StringVar(&port, "port", os.Getenv("CMON_PORT"), "server listen port")
	flag.Parse()
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.Handle("/api", httpMonitor(hrAPI))
	mux.Handle("/test", httpMonitor(hrTEST))
	srv = &http.Server{
		Addr:           ":" + port,
		Handler:        mux,
		ReadTimeout:    12 * time.Second,
		WriteTimeout:   12 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	cObj = map[string]*obj{
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
			w.Write([]byte("api response"))
		case hrTEST:
			w.Write([]byte("test response"))
		}
	}
}

func objManage(o *obj, n string, ctl chan string) {
	var or objRq
	var accessors, token uint32
	o.req = make(chan objRq, 16)
	o.rel = make(chan uint32, 16)
	o.boot(n, ctl)

	// loop indefinitely as object access manager when boot complete
	for ; ; token++ {
		for or = <-o.req; or.rt&rtEXCL == 0; token++ {
			or.acc <- token
			for accessors++; ; accessors-- {
				select {
				case or = <-o.req:
					break
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

func main() {
	ctl := make(chan string, 4)
	for n, o := range cObj {
		go objManage(o, n, ctl)
	}
	for i := 0; i < len(cObj); i++ {
		n := <-ctl
		cObj[n].stat = osINIT
		log.Printf("%q object booted", n)
	}

	for n, o := range cObj {
		go o.maint(n)
	}
	go func() {
		log.Printf("listening on port %v for HTTP requests", srv.Addr[1:])
		switch err := srv.ListenAndServe(); err {
		case nil:
		case http.ErrServerClosed:
		default:
			log.Fatalf("cannot listen for HTTP requests: %v", err)
		}
		log.Printf("stopped listening for HTTP requests")
		sig <- nil
	}()

	log.Printf("signal %v received: beginning shutdown", <-sig)
	srv.Close() // context/srv.Shutdown() more graceful alternative
	for n, o := range cObj {
		go o.term(n, ctl)
	}
	for i := 0; i < len(cObj); i++ {
		n := <-ctl
		cObj[n].stat = osTERM
		log.Printf("%q object shutdown", n)
	}
	log.Printf("graceful shutdown complete")
}
