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
	hrADMIN httpRq = iota
	hrAPI0
	hrVM0
	hrDISK0
	hrDB0
	hrAPI1
	hrVM1
	hrDISK1
	hrDB1
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
	sig                    chan os.Signal
	rqID, rqS, rqE         chan int64
	port                   string
	srv                    *http.Server
	cObj                   map[string]*obj
	logD, logI, logW, logE *log.Logger
	exit, rqOpen           int
	rqCount                int64
)

func init() {
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	log.SetFlags(0)
	logD = log.New(os.Stderr, "DEBUG ", log.Lshortfile|log.Lmicroseconds)
	logI = log.New(os.Stderr, "", 0)
	logW = log.New(os.Stderr, "WARNING ", log.Lshortfile)
	logE = log.New(os.Stderr, "ERROR ", log.Lshortfile)

	flag.StringVar(&port, "port", os.Getenv("CMON_PORT"), "server listen port")
	flag.Parse()
	if port == "" {
		port = "4404"
	}

	rqID, rqS, rqE = make(chan int64, 16), make(chan int64, 16), make(chan int64, 16)
	mux := http.NewServeMux()
	mux.Handle("/admin", httpMonitor(hrADMIN))
	mux.Handle("/api/v0", httpMonitor(hrAPI0))
	mux.Handle("/api/v0/vms", httpMonitor(hrVM0))
	mux.Handle("/api/v0/disks", httpMonitor(hrDISK0))
	mux.Handle("/api/v0/dbs", httpMonitor(hrDB0))
	mux.Handle("/api/v1", httpMonitor(hrAPI1))
	mux.Handle("/api/v1/vms", httpMonitor(hrVM1))
	mux.Handle("/api/v1/disks", httpMonitor(hrDISK1))
	mux.Handle("/api/v1/dbs", httpMonitor(hrDB1))
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
		id := <-rqID
		switch rqS <- id; hr {
		case hrADMIN:
			w.Write([]byte("admin stub response"))
		case hrAPI0:
			// map to rmonitor
			// select inputs from accessor(s) result(s) & http.CloseNotifier channels
			w.Write([]byte("APIv0 stub response"))
		case hrVM0:
			w.Write([]byte("APIv0 VMs stub response"))
		case hrDISK0:
			w.Write([]byte("APIv0 disks stub response"))
		case hrDB0:
			w.Write([]byte("APIv0 DBs stub response"))
		case hrAPI1:
			w.Write([]byte("APIv1 stub response"))
		case hrVM1:
			w.Write([]byte("APIv1 VMs stub response"))
		case hrDISK1:
			w.Write([]byte("APIv1 disks stub response"))
		case hrDB1:
			w.Write([]byte("APIv1 DBs stub response"))
		}
		rqE <- id
	}
}

func objManage(o *obj, n string, ctl chan string) {
	var or objRq
	var accessors, token uint32
	o.req = make(chan objRq, 16)
	o.rel = make(chan uint32, 16)
	o.boot(n, ctl)

	for ; ; token++ { // loop indefinitely as object access manager when boot complete
	nextRequest:
		for or = <-o.req; or.rt&rtEXCL == 0; token++ {
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

func rqMonitor() {
	for {
		select {
		case <-rqS:
			rqOpen++
		case <-rqE:
			rqOpen--
		case rqID <- rqCount:
			rqCount++
		}
	}
}

func main() {
	logI.Printf("booting %v monitored objects", len(cObj))
	ctl := make(chan string, 4)
	for n, o := range cObj {
		go objManage(o, n, ctl)
	}
	for i := 0; i < len(cObj); i++ {
		n := <-ctl
		o := cObj[n]
		o.stat = osINIT
		logI.Printf("%q object booted", n)
		go o.maint(n)
	}

	logI.Printf("listening on port %v for HTTP requests", srv.Addr[1:])
	go func() {
		switch s := <-sig; s {
		case syscall.SIGINT, syscall.SIGTERM:
			logI.Printf("beginning signaled shutdown")
		case nil:
		default:
			logE.Printf("beginning shutdown on unexpected %v signal", s)
			exit = 1
		}
		for n, o := range cObj {
			go o.term(n, ctl)
		}
		time.Sleep(1250 * time.Millisecond)
		srv.Close()
	}()
	go rqMonitor()
	switch err := srv.ListenAndServe(); err {
	case nil, http.ErrServerClosed:
		logI.Printf("stopped listening for HTTP requests (%v open)", rqOpen)
	default:
		logE.Printf("beginning shutdown on HTTP listener failure (%v)", err)
		exit = 1
	}
	sig <- nil

	for i := 0; i < len(cObj); i++ {
		n := <-ctl
		cObj[n].stat = osTERM
		logI.Printf("%q object shutdown", n)
	}
	logI.Printf("shutdown complete with %v requests handled", <-rqID)
	os.Exit(exit)
}
