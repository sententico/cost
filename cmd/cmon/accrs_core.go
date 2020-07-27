package main

import (
	"log"
	"time"
)

const (
	s90 = 90 * time.Second
	s6  = 6 * time.Second
)

func (o *obj) boot(n string, ctl chan string) {
	switch n {
	case "ec2":
		ec2Boot(o)
	case "rds":
		rdsBoot(o)
	}
	ctl <- n
}
func (o *obj) maint(n string) {
	switch n {
	case "ec2":
		ec2Maint(o)
	case "rds":
		rdsMaint(o)
	}
	log.Printf("%q object maintenance failed", n)
}
func (o *obj) term(n string, ctl chan string) {
	switch n {
	case "ec2":
		ec2Term(o)
	case "rds":
		rdsTerm(o)
	}
	ctl <- n
}

func ec2Boot(o *obj) {
	// read/build object
	o.data = nil
}
func ec2MaintS(o *obj, acc chan uint32) {
	o.req <- objRq{0, acc}
	token := <-acc
	// shared access maintenance
	o.rel <- token
}
func ec2MaintX(o *obj, acc chan uint32) {
	o.req <- objRq{rtEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	o.rel <- token
}
func ec2Maint(o *obj) {
	for acc, st, xt := make(chan uint32, 1), time.NewTicker(s6), time.NewTicker(s90); ; {
		select {
		case <-st.C:
			ec2MaintS(o, acc)
		case <-xt.C:
			ec2MaintX(o, acc)
		}
	}
}
func ec2Term(o *obj) {
	or := objRq{rtEXCL, make(chan uint32, 1)}
	o.req <- or
	<-or.acc
	// persist object for shutdown; term accessors don't release object
}

func rdsBoot(o *obj) {
	// read/build object
	o.data = nil
}
func rdsMaintS(o *obj, acc chan uint32) {
	o.req <- objRq{0, acc}
	token := <-acc
	// shared access maintenance
	o.rel <- token
}
func rdsMaintX(o *obj, acc chan uint32) {
	o.req <- objRq{rtEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	o.rel <- token
}
func rdsMaint(o *obj) {
	for acc, st, xt := make(chan uint32, 1), time.NewTicker(s6), time.NewTicker(s90); ; {
		select {
		case <-st.C:
			rdsMaintS(o, acc)
		case <-xt.C:
			rdsMaintX(o, acc)
		}
	}
}
func rdsTerm(o *obj) {
	or := objRq{rtEXCL, make(chan uint32, 1)}
	o.req <- or
	<-or.acc
	// persist object for shutdown; term accessors don't release object
}
