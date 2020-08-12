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
	m := mMod[n]
	// read/build object model
	m.data = objEC2{
		"i-dog": {"m5.2xlarge", "linux"},
		"i-cat": {"m5.large", "DOS"},
	}
	ctl <- n
}
func ec2awsMaintS(m *model, acc chan uint32) {
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func ec2awsMaintX(m *model, acc chan uint32) {
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	m.rel <- token
}
func ec2awsMaint(n string) {
	for m, acc, st, xt := mMod[n], make(chan uint32, 1),
		time.NewTicker(6*time.Second), time.NewTicker(90*time.Second); ; {
		select {
		case <-st.C:
			ec2awsMaintS(m, acc)
		case <-xt.C:
			ec2awsMaintX(m, acc)
		}
	}
}
func ec2awsTerm(n string, ctl chan string) {
	m := mMod[n]
	mr := modRq{atEXCL, make(chan uint32, 1)}
	m.req <- mr
	<-mr.acc
	// persist object model for shutdown; term accessors don't release object
	ctl <- n
}

func rdsawsBoot(n string, ctl chan string) {
	m := mMod[n]
	// read/build object model
	m.data = nil
	ctl <- n
}
func rdsawsMaintS(m *model, acc chan uint32) {
	m.req <- modRq{0, acc}
	token := <-acc
	// shared access maintenance
	m.rel <- token
}
func rdsawsMaintX(m *model, acc chan uint32) {
	m.req <- modRq{atEXCL, acc}
	token := <-acc
	// exclusive access maintenance
	m.rel <- token
}
func rdsawsMaint(n string) {
	for m, acc, st, xt := mMod[n], make(chan uint32, 1),
		time.NewTicker(6*time.Second), time.NewTicker(90*time.Second); ; {
		select {
		case <-st.C:
			rdsawsMaintS(m, acc)
		case <-xt.C:
			rdsawsMaintX(m, acc)
		}
	}
}
func rdsawsTerm(n string, ctl chan string) {
	m := mMod[n]
	mr := modRq{atEXCL, make(chan uint32, 1)}
	m.req <- mr
	<-mr.acc
	// persist object model for shutdown; term accessors don't release object
	ctl <- n
}
