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
	httpSe uint8
	objSt  uint8
	accTyp uint8
	objRq  struct {
		typ accTyp
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
	hsADMIN httpSe = iota
	hsAPI0
	hsVM0
	hsDISK0
	hsDB0
	hsAPI1
	hsVM1
	hsDISK1
	hsDB1
)
const (
	osNIL objSt = iota
	osINIT
	osTERM
)
const (
	atEXCL accTyp = 1 << iota
	atLONG
	atPRI
)

var (
	sig                    chan os.Signal
	seID, seS, seE         chan int64
	port                   string
	srv                    *http.Server
	cObj                   map[string]*obj
	logD, logI, logW, logE *log.Logger
	exit, seOpen           int
	seCount                int64
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

	seID, seS, seE = make(chan int64, 16), make(chan int64, 16), make(chan int64, 16)
	mux := http.NewServeMux()
	mux.Handle("/admin", httpSession(hsADMIN))
	mux.Handle("/api/v0", httpSession(hsAPI0))
	mux.Handle("/api/v0/vms", httpSession(hsVM0))
	mux.Handle("/api/v0/disks", httpSession(hsDISK0))
	mux.Handle("/api/v0/dbs", httpSession(hsDB0))
	mux.Handle("/api/v1", httpSession(hsAPI1))
	mux.Handle("/api/v1/vms", httpSession(hsVM1))
	mux.Handle("/api/v1/disks", httpSession(hsDISK1))
	mux.Handle("/api/v1/dbs", httpSession(hsDB1))
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

func httpSession(hs httpSe) http.HandlerFunc { // pass in args for closure to close over
	return func(w http.ResponseWriter, r *http.Request) {
		id := <-seID
		switch seS <- id; hs {
		case hsADMIN:
			w.Write([]byte("admin stub response"))
		case hsAPI0:
			// map to hsession
			// select inputs from accessor(s) result(s) & http.CloseNotifier channels
			w.Write([]byte("APIv0 stub response"))
		case hsVM0:
			w.Write([]byte("APIv0 VMs stub response"))
		case hsDISK0:
			w.Write([]byte("APIv0 disks stub response"))
		case hsDB0:
			w.Write([]byte("APIv0 DBs stub response"))
		case hsAPI1:
			w.Write([]byte("APIv1 stub response"))
		case hsVM1:
			w.Write([]byte("APIv1 VMs stub response"))
		case hsDISK1:
			w.Write([]byte("APIv1 disks stub response"))
		case hsDB1:
			w.Write([]byte("APIv1 DBs stub response"))
		}
		seE <- id
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

func seMonitor() {
	var lc int64
	for s, t := seS, time.NewTicker(60000*time.Millisecond); ; {
		select {
		case <-t.C:
			if nc := seCount - int64(len(seID)); nc > lc {
				logI.Printf("handled %v sessions", nc-lc)
				lc = nc
			}
		case <-s:
			// seS may be set to nil to stop new sessions
			seOpen++
		case <-seE:
			seOpen--
		case seID <- seCount:
			seCount++
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
	go seMonitor()
	switch err := srv.ListenAndServe(); err {
	case nil, http.ErrServerClosed:
		logI.Printf("stopped listening for HTTP requests (%v sessions open)", seOpen)
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
	logI.Printf("shutdown complete with %v sessions handled", seCount-int64(len(seID)))
	os.Exit(exit)
}
