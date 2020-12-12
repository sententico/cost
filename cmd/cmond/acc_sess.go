package main

import (
	"fmt"
	"net/url"
)

func ec2awsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if inst := m.data[1].(*ec2Detail).Inst[id]; inst != nil {
		s = fmt.Sprintf("%v", *inst)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func ebsawsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if vol := m.data[1].(*ebsDetail).Vol[id]; vol != nil {
		s = fmt.Sprintf("%v", *vol)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func rdsawsLookup(m *model, v url.Values, res chan<- interface{}) {
	// prepare lookup and validate v; even on error return a result on res
	id, acc := v.Get("id"), m.newAcc()

	// perform read access EC2 lookup, copying results before release
	acc.reqR()
	var s string
	if db := m.data[1].(*rdsDetail).DB[id]; db != nil {
		s = fmt.Sprintf("%v", *db)
	}
	acc.rel()

	// after releasing access to object model, create and send result
	res <- s
}

func cdraspMetrics() (res chan interface{}, err error) {
	acc := mMod["cdr.asp"].newAcc()
	// check parameters; on error: return nil, fmt.Errorf()
	res = make(chan interface{}, 1)
	go func() {
		defer func() {
			switch e := recover(); {
			case e == nil:
			case e.(error).Error() != "send on closed channel":
				close(res)
				fallthrough
			default:
				acc.rel()
			}
		}()
		acc.reqR()
		// gather results and send to res
		acc.rel()
		res <- nil
		close(res)
	}()
	return
}
