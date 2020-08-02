package main

import (
	"time"
)

type (
	ec2Inst struct {
		Typ, OS string
	}
	objEC2 map[string]*ec2Inst
)

func ec2awsBoot(n string, ctl chan string) {
	o := mObj[n]
	// read/build object
	o.data = objEC2{
		"i-dog": {"m5.2xlarge", "linux"},
		"i-cat": {"m5.large", "DOS"},
	}
	ctl <- n
}
func ec2awsMaintS(o *obj, acc chan uint32) {
	o.req <- objRq{0, acc}
	token := <-acc
	// shared access maintenance
	o.rel <- token
}
func ec2awsMaintX(o *obj, acc chan uint32) {
	o.req <- objRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	o.rel <- token
}
func ec2awsMaint(n string) {
	for o, acc, st, xt := mObj[n], make(chan uint32, 1),
		time.NewTicker(6*time.Second), time.NewTicker(90*time.Second); ; {
		select {
		case <-st.C:
			ec2awsMaintS(o, acc)
		case <-xt.C:
			ec2awsMaintX(o, acc)
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	o := mObj[n]
	or := objRq{atEXCL, make(chan uint32, 1)}
	o.req <- or
	<-or.acc
	// persist object for shutdown; term accessors don't release object
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	o := mObj[n]
	// read/build object
	o.data = nil
	ctl <- n
}
func rdsawsMaintS(o *obj, acc chan uint32) {
	o.req <- objRq{0, acc}
	token := <-acc
	// shared access maintenance
	o.rel <- token
}
func rdsawsMaintX(o *obj, acc chan uint32) {
	o.req <- objRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	o.rel <- token
}
func rdsawsMaint(n string) {
	for o, acc, st, xt := mObj[n], make(chan uint32, 1),
		time.NewTicker(6*time.Second), time.NewTicker(90*time.Second); ; {
		select {
		case <-st.C:
			rdsawsMaintS(o, acc)
		case <-xt.C:
			rdsawsMaintX(o, acc)
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	o := mObj[n]
	or := objRq{atEXCL, make(chan uint32, 1)}
	o.req <- or
	<-or.acc
	// persist object for shutdown; term accessors don't release object
	ctl <- n
}
